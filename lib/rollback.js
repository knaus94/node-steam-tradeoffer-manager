"use strict";

module.exports = function (TradeOfferManager) {
	const ROLLBACK_STATUS_CODE = 12; // trade reversed in history

	const DEFAULT_ROLLBACK_POLL_INTERVAL = 60 * 1000; // ms
	const DEFAULT_ROLLBACK_WINDOW_MS = 8 * 24 * 60 * 60 * 1000; // 8 days
	const DEFAULT_HISTORY_PAGE_SIZE = 500;

	// time-driven knobs
	const DEFAULT_MAX_SCAN_MS = 1500; // per cycle time budget
	const DEFAULT_SCAN_MARGIN_SEC = 300; // safety window below oldest target (5 min)
	const DEFAULT_ANCHORS_MAX = 1024; // max anchors stored
	const DEFAULT_ANCHOR_GAP_SEC = 1800; // minimal time distance between anchors (30 min)
	const DEFAULT_MAX_WATCH_CHECKS = 200; // cap how many watch entries we try per cycle

	// ---------- state helpers ----------

	TradeOfferManager.prototype._ensureRollbackState = function () {
		if (
			!this.rollbackData ||
			typeof this.rollbackData !== "object" ||
			!this.rollbackData.watch ||
			!this.rollbackData.offerToTrade ||
			!Object.prototype.hasOwnProperty.call(this.rollbackData, "cursor")
		) {
			this.rollbackData = {
				watch: Object.create(null), // tradeid -> { offerId, acceptedAt, ... }
				offerToTrade: Object.create(null), // offerId -> tradeid
				cursor: null, // last deep cursor (optional)
				anchors: [], // [{ t: time_init (seconds), id: tradeid }]
			};
		} else if (!Array.isArray(this.rollbackData.anchors)) {
			this.rollbackData.anchors = [];
		}
	};

	TradeOfferManager.prototype._persistRollbackBlock = function () {
		this.emit("rollbackData", this.rollbackData);
	};

	TradeOfferManager.prototype._ensureRollbackTimer = function (reason) {
		if (!this.rollbackEnabled) return;
		this._ensureRollbackState();
		const rb = this.rollbackData;
		if (!rb || Object.keys(rb.watch).length === 0) return; // nothing to track
		this.emit(
			"debug",
			`rollback: ensure timer (${reason || "unspecified"})`
		);
		this._startRollbackTimer(0);
	};

	TradeOfferManager.prototype.setRollbackData = function (data, options) {
		const startTimer =
			options && typeof options.startTimer === "boolean"
				? options.startTimer
				: true;
		this._ensureRollbackState();
		if (!data || typeof data !== "object") return;

		this.rollbackData = {
			watch:
				data.watch && typeof data.watch === "object" ? data.watch : {},
			offerToTrade:
				data.offerToTrade && typeof data.offerToTrade === "object"
					? data.offerToTrade
					: {},
			cursor: Object.prototype.hasOwnProperty.call(data, "cursor")
				? data.cursor || null
				: null,
			anchors: Array.isArray(data.anchors) ? data.anchors : [],
		};

		this._persistRollbackBlock();
		if (startTimer) this._ensureRollbackTimer("setRollbackData");
	};

	TradeOfferManager.prototype.configureRollbackTracking = function (opts) {
		this.rollbackEnabled =
			typeof opts?.enabled !== "undefined"
				? !!opts.enabled
				: this.rollbackEnabled;

		this.rollbackPollInterval =
			opts?.pollInterval ||
			this.rollbackPollInterval ||
			DEFAULT_ROLLBACK_POLL_INTERVAL;
		this.rollbackWindowMs =
			opts?.windowMs ||
			this.rollbackWindowMs ||
			DEFAULT_ROLLBACK_WINDOW_MS;
		this.rollbackHistoryPageSize =
			opts?.pageSize ||
			this.rollbackHistoryPageSize ||
			DEFAULT_HISTORY_PAGE_SIZE;

		this.rollbackMaxScanMsPerCycle =
			typeof opts?.maxScanMsPerCycle === "number"
				? opts.maxScanMsPerCycle
				: this.rollbackMaxScanMsPerCycle || DEFAULT_MAX_SCAN_MS;

		this.rollbackScanMarginSec =
			typeof opts?.scanMarginSec === "number"
				? opts.scanMarginSec
				: this.rollbackScanMarginSec || DEFAULT_SCAN_MARGIN_SEC;

		this.rollbackAnchorsMax =
			typeof opts?.anchorsMax === "number"
				? opts.anchorsMax
				: this.rollbackAnchorsMax || DEFAULT_ANCHORS_MAX;

		this.rollbackAnchorGapSec =
			typeof opts?.anchorGapSec === "number"
				? opts.anchorGapSec
				: this.rollbackAnchorGapSec || DEFAULT_ANCHOR_GAP_SEC;

		this.rollbackMaxWatchChecksPerCycle =
			typeof opts?.maxWatchChecksPerCycle === "number"
				? opts.maxWatchChecksPerCycle
				: this.rollbackMaxWatchChecksPerCycle ||
				  DEFAULT_MAX_WATCH_CHECKS;

		this._ensureRollbackState();

		if (this.rollbackEnabled) {
			this._ensureRollbackTimer("configureRollbackTracking");
		} else {
			clearTimeout(this._rollbackTimer);
			this._rollbackTimer = null;
		}
	};

	/**
	 * Only add a watch on real transition to Accepted.
	 * polling.js must gate this via pollData.offerMeta (see patch in the answer).
	 */
	TradeOfferManager.prototype._onOfferAccepted = function (offer) {
		if (
			!offer ||
			!offer.isOurOffer ||
			offer.state !== TradeOfferManager.ETradeOfferState.Accepted ||
			!offer.tradeID
		) {
			return;
		}

		// Secondary protection: ignore if the same Accept we've already seen in this pollData
		const updatedSec = Math.floor(
			(offer.updated ? offer.updated.getTime() : Date.now()) / 1000
		);
		const meta =
			this.pollData &&
			this.pollData.offerMeta &&
			this.pollData.offerMeta[offer.id];
		if (
			meta &&
			meta.lastState === TradeOfferManager.ETradeOfferState.Accepted &&
			meta.lastUpdatedSec === updatedSec
		) {
			return;
		}

		this._ensureRollbackState();
		const rb = this.rollbackData;

		const tid = String(offer.tradeID);
		if (rb.watch[tid]) return; // idempotent

		const acceptedAtMs = offer.updated
			? offer.updated.getTime()
			: Date.now();

		rb.watch[tid] = {
			offerId: offer.id,
			tradeId: tid,
			acceptedAt: acceptedAtMs,
			until:
				acceptedAtMs +
				(this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS),
			lastSeenStatus: TradeOfferManager.ETradeOfferState.Accepted,
			lastChecked: 0,
		};
		rb.offerToTrade[offer.id] = tid;

		this._persistRollbackBlock();
		this._ensureRollbackTimer("offerAccepted");
	};

	TradeOfferManager.prototype._startRollbackTimer = function (delayMs) {
		if (!this.rollbackEnabled) return;
		clearTimeout(this._rollbackTimer);
		this._rollbackTimer = setTimeout(
			this._runRollbackCycle.bind(this),
			typeof delayMs === "number"
				? delayMs
				: this.rollbackPollInterval || DEFAULT_ROLLBACK_POLL_INTERVAL
		);
	};

	// ---------- anchors helpers ----------

	function _rbInsertAnchorSorted(rb, tSec, tradeId, gapSec, maxAnchors) {
		// Keep anchors sorted ascending by time (older -> newer)
		const anchors = rb.anchors || (rb.anchors = []);
		// prune by time gap to avoid near-duplicates
		const len = anchors.length;
		if (len > 0) {
			const last = anchors[len - 1]; // newest currently
			if (Math.abs(last.t - tSec) < gapSec) {
				// too close to previous recorded anchor, skip
				return;
			}
		}
		// simple insert keeping ascending order (anchors array is small)
		let inserted = false;
		for (let i = 0; i < anchors.length; i++) {
			if (tSec < anchors[i].t) {
				anchors.splice(i, 0, { t: tSec, id: String(tradeId) });
				inserted = true;
				break;
			}
		}
		if (!inserted) anchors.push({ t: tSec, id: String(tradeId) });

		// cap by size (drop oldest)
		if (anchors.length > maxAnchors) {
			anchors.splice(0, anchors.length - maxAnchors);
		}
	}

	function _rbFindAnchorForTime(rb, targetSec) {
		// Return the smallest anchor with t >= targetSec (i.e., just above the time we want)
		const anchors = rb.anchors || [];
		for (let i = 0; i < anchors.length; i++) {
			if (anchors[i].t >= targetSec) {
				return {
					start_after_time: anchors[i].t,
					start_after_tradeid: anchors[i].id,
				};
			}
		}
		return null; // no suitable anchor; will start from head
	}

	function _rbPruneAnchors(rb, cutoffSec) {
		// Remove anchors that are older than cutoffSec (outside monitoring window)
		if (!rb.anchors || rb.anchors.length === 0) return;
		rb.anchors = rb.anchors.filter((a) => a.t >= cutoffSec);
	}

	// ---------- core cycle (single targeted range) ----------

	TradeOfferManager.prototype._runRollbackCycle = function () {
		if (!this.rollbackEnabled) return;

		if (this._rollbackBusy) {
			this._startRollbackTimer(this.rollbackPollInterval);
			return;
		}
		this._rollbackBusy = true;

		this._ensureRollbackState();
		const rb = this.rollbackData;

		const cycleStart = Date.now();
		this.emit("debug", "rollback: cycle start");

		const now = Date.now();
		const nowSec = Math.floor(now / 1000);
		const windowMs = this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS;
		const cutoffSec = Math.floor((now - windowMs) / 1000) - 10 * 60; // extra 10-min buffer like before

		// 0) GC expired watches
		let changed = false;
		for (const tid in rb.watch) {
			if (!Object.prototype.hasOwnProperty.call(rb.watch, tid)) continue;
			const w = rb.watch[tid];
			if (w.until <= now) {
				if (
					w.offerId != null &&
					Object.prototype.hasOwnProperty.call(
						rb.offerToTrade,
						w.offerId
					)
				) {
					delete rb.offerToTrade[w.offerId];
				}
				delete rb.watch[tid];
				changed = true;
			}
		}
		if (changed) this._persistRollbackBlock();

		const allWatchIds = Object.keys(rb.watch);
		if (allWatchIds.length === 0) {
			this._rollbackBusy = false;
			this.emit(
				"debug",
				`rollback: cycle end (no watchers) in ${
					Date.now() - cycleStart
				}ms`
			);
			this._startRollbackTimer(this.rollbackPollInterval * 2);
			return;
		}

		// 1) Select a bounded subset of watch to scan in this cycle.
		//    Strategy: take up to rollbackMaxWatchChecksPerCycle entries with widest time span but compact range.
		const maxChecks =
			this.rollbackMaxWatchChecksPerCycle || DEFAULT_MAX_WATCH_CHECKS;
		// Sort by acceptedAt desc (newest first)
		const sorted = allWatchIds
			.map((tid) => ({
				tid,
				acceptedAtSec: Math.floor(
					(rb.watch[tid].acceptedAt || now) / 1000
				),
			}))
			.sort((a, b) => b.acceptedAtSec - a.acceptedAtSec);

		// Take first K entries (newest); this makes scan range [min..max] naturally compact near head.
		const selected = sorted.slice(
			0,
			Math.max(1, Math.min(maxChecks, sorted.length))
		);
		const watchSet = new Set(selected.map((x) => x.tid));
		const rangeMaxSec = selected[0].acceptedAtSec;
		const rangeMinSec = selected.reduce(
			(m, x) => Math.min(m, x.acceptedAtSec),
			selected[0].acceptedAtSec
		);

		const stopTimeSec = Math.max(
			cutoffSec,
			rangeMinSec -
				(this.rollbackScanMarginSec || DEFAULT_SCAN_MARGIN_SEC)
		);

		// 2) Choose start anchor: nearest anchor just above rangeMaxSec; if none — start from head (null)
		_rbPruneAnchors(rb, cutoffSec);
		const startAnchor = _rbFindAnchorForTime(rb, rangeMaxSec);

		const pageSize =
			this.rollbackHistoryPageSize || DEFAULT_HISTORY_PAGE_SIZE;
		const timeBudgetMs =
			this.rollbackMaxScanMsPerCycle || DEFAULT_MAX_SCAN_MS;
		const gapSec = this.rollbackAnchorGapSec || DEFAULT_ANCHOR_GAP_SEC;
		const anchorsMax = this.rollbackAnchorsMax || DEFAULT_ANCHORS_MAX;

		const found = Object.create(null);
		let foundCount = 0;

		let options = {
			max_trades: pageSize,
			include_failed: 1,
			include_total: 0,
			get_descriptions: 0,
		};
		if (startAnchor) {
			options.start_after_time = startAnchor.start_after_time;
			options.start_after_tradeid = startAnchor.start_after_tradeid;
		}

		const scanStart = Date.now();
		let pages = 0;

		const step = () => {
			this._apiCall("GET", "GetTradeHistory", 1, options, (err, body) => {
				if (err || !body || !body.response) {
					this.emit(
						"debug",
						"GetTradeHistory error: " +
							(err ? err.message : "malformed")
					);
					return finalize(); // end this cycle gracefully
				}

				const trades = Array.isArray(body.response.trades)
					? body.response.trades
					: [];
				if (trades.length === 0) {
					return finalize();
				}

				// Visit trades and collect matches + anchors
				for (let i = 0; i < trades.length; i++) {
					const t = trades[i];
					const tid = String(t.tradeid);
					if (
						watchSet.has(tid) &&
						!Object.prototype.hasOwnProperty.call(found, tid)
					) {
						found[tid] = t;
						foundCount++;
					}
				}

				const last = trades[trades.length - 1];
				const lastTime = last.time_init;

				// record anchor from the tail of this page (for future jumps)
				_rbInsertAnchorSorted(
					rb,
					lastTime,
					String(last.tradeid),
					gapSec,
					anchorsMax
				);

				// advance cursor for next page
				options.start_after_time = lastTime;
				options.start_after_tradeid = String(last.tradeid);

				pages++;
				const elapsed = Date.now() - scanStart;

				const stopByTime =
					typeof lastTime === "number" && lastTime < stopTimeSec;
				const stopByBudget = elapsed >= timeBudgetMs;
				const stopByGoal = foundCount >= watchSet.size;

				if (
					!stopByTime &&
					!stopByBudget &&
					!stopByGoal &&
					body.response.more
				) {
					return step();
				} else {
					return finalize();
				}
			});
		};

		const finalize = () => {
			let anyChange = false;

			for (const tid in found) {
				if (!Object.prototype.hasOwnProperty.call(found, tid)) continue;
				const entry = found[tid];
				const st = entry.status;

				if (st === ROLLBACK_STATUS_CODE) {
					const watch = rb.watch[tid];
					if (watch) {
						this.getOffer(watch.offerId, (err, offer) => {
							const payloadOffer = err
								? {
										id: watch.offerId,
										tradeID: tid,
										state: TradeOfferManager
											.ETradeOfferState.Accepted,
										isOurOffer: true,
								  }
								: offer;
							this.emit("tradeRolledBack", payloadOffer, entry);
						});
						delete rb.watch[tid];
						if (
							watch.offerId != null &&
							Object.prototype.hasOwnProperty.call(
								rb.offerToTrade,
								watch.offerId
							)
						) {
							delete rb.offerToTrade[watch.offerId];
						}
						anyChange = true;
					}
				} else {
					const w = rb.watch[tid];
					if (w) {
						w.lastSeenStatus = st;
						w.lastChecked = Date.now();
					}
				}
			}

			// Save deep cursor for continuity (optional)
			if (options.start_after_time && options.start_after_tradeid) {
				rb.cursor = {
					start_after_time: options.start_after_time,
					start_after_tradeid: options.start_after_tradeid,
				};
			}

			if (anyChange) this._persistRollbackBlock();

			this._rollbackBusy = false;
			this.emit(
				"debug",
				`rollback: cycle end in ${
					Date.now() - cycleStart
				}ms (pages=${pages}, selected=${
					watchSet.size
				}, found=${foundCount})`
			);
			this._startRollbackTimer(
				this.rollbackPollInterval || DEFAULT_ROLLBACK_POLL_INTERVAL
			);
		};

		step();
	};
};
