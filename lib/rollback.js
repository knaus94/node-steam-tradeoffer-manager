"use strict";

module.exports = function (TradeOfferManager) {
	const ROLLBACK_STATUS_CODE = 12; // trade reversed in history

	const DEFAULT_ROLLBACK_POLL_INTERVAL = 60 * 1000; // ms
	const DEFAULT_HISTORY_PAGE_SIZE = 500;

	// Time-driven knobs (tunable)
	const DEFAULT_MAX_SCAN_MS = 1500; // per-cycle scan budget
	const DEFAULT_SCAN_MARGIN_SEC = 300; // extra time below oldest selected watch (5 min)
	const DEFAULT_ANCHORS_MAX = 1024; // cap anchors stored
	const DEFAULT_ANCHOR_GAP_SEC = 1800; // ~30 min gap between anchors
	const DEFAULT_MAX_WATCH_CHECKS = 200; // how many watch entries to cover per cycle

	// Slack for pruning state relative to oldest active watch deadline (12h)
	const DEFAULT_PRUNE_SLACK_MS = 12 * 60 * 60 * 1000;

	// ---------- UK DST helpers (BST/GMT) ----------
	function lastSundayUTC(year, monthIndex) {
		// monthIndex: 0..11; return ms at 00:00 UTC of last Sunday in month
		const d = new Date(Date.UTC(year, monthIndex + 1, 0, 0, 0, 0, 0)); // last day of month
		const weekday = d.getUTCDay(); // 0..6; 0=Sunday
		const delta = weekday; // days to go back to Sunday
		d.setUTCDate(d.getUTCDate() - delta);
		return d.getTime();
	}

	function bstStartUTC(year) {
		// BST starts at 01:00 UTC on the last Sunday in March
		const base = lastSundayUTC(year, 2); // March
		return Date.UTC(year, 2, new Date(base).getUTCDate(), 1, 0, 0, 0);
	}

	function bstEndUTC(year) {
		// BST ends at 01:00 UTC on the last Sunday in October
		const base = lastSundayUTC(year, 9); // October
		return Date.UTC(year, 9, new Date(base).getUTCDate(), 1, 0, 0, 0);
	}

	function isBSTAtUTC(ms) {
		const d = new Date(ms);
		const y = d.getUTCFullYear();
		const start = bstStartUTC(y);
		const end = bstEndUTC(y);
		return ms >= start && ms < end;
	}

	function dayBoundaryUTCFor(ms) {
		// Returns ms at boundary time for the given day: 07:00 UTC during BST, 08:00 UTC during GMT.
		const d = new Date(ms);
		const y = d.getUTCFullYear();
		const m = d.getUTCMonth();
		const day = d.getUTCDate();
		const midnight = Date.UTC(y, m, day, 0, 0, 0, 0);
		// Use midday to decide DST status safely for that calendar day
		const midday = midnight + 12 * 60 * 60 * 1000;
		const hour = isBSTAtUTC(midday) ? 7 : 8;
		return midnight + hour * 60 * 60 * 1000;
	}

	// 7 full days + next 07:00 (BST) / 08:00 (GMT) boundary
	function computeSettlementDeadlineMs(acceptedAtMs) {
		const base = acceptedAtMs + 7 * 24 * 60 * 60 * 1000;
		const sameDayBoundary = dayBoundaryUTCFor(base);
		if (base < sameDayBoundary) {
			return sameDayBoundary;
		}
		// Next day boundary
		return dayBoundaryUTCFor(base + 24 * 60 * 60 * 1000);
	}

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
				watch: Object.create(null), // tradeid -> { offerId, acceptedAt, until, ... }
				offerToTrade: Object.create(null), // offerId -> tradeid
				rolled: Object.create(null), // tradeid -> time_mod (sec)
				anchors: [], // [{ t: time_init (seconds), id: tradeid }] sorted asc
				cursor: null, // last scanned page tail (debug/optional)
				pickOffset: 0, // rotating offset over sorted watch list (fair coverage)
			};
		} else {
			if (!this.rollbackData.rolled)
				this.rollbackData.rolled = Object.create(null);
			if (!Array.isArray(this.rollbackData.anchors))
				this.rollbackData.anchors = [];
			if (typeof this.rollbackData.pickOffset !== "number")
				this.rollbackData.pickOffset = 0;
		}

		// Normalize "rolled" to numeric timestamps so that prune works reliably
		const rb = this.rollbackData;
		const nowSec = Math.floor(Date.now() / 1000);
		let normalized = false;
		for (const tid of Object.keys(rb.rolled)) {
			const v = rb.rolled[tid];
			if (typeof v !== "number" || !isFinite(v) || v <= 0) {
				rb.rolled[tid] = nowSec;
				normalized = true;
			}
		}
		if (normalized) this.emit("debug", "rollback: normalized rolled map");
	};

	TradeOfferManager.prototype._persistRollbackBlock = function () {
		this.emit("rollbackData", this.rollbackData);
	};

	TradeOfferManager.prototype._ensureRollbackTimer = function (reason) {
		if (!this.rollbackEnabled) return;
		this._ensureRollbackState();
		const rb = this.rollbackData;
		if (!rb || Object.keys(rb.watch).length === 0) return;
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
			rolled:
				data.rolled && typeof data.rolled === "object"
					? data.rolled
					: Object.create(null),
			anchors: Array.isArray(data.anchors) ? data.anchors : [],
			cursor: Object.prototype.hasOwnProperty.call(data, "cursor")
				? data.cursor || null
				: null,
			pickOffset:
				typeof data.pickOffset === "number" ? data.pickOffset : 0,
		};

		// Normalize in case external data contains booleans
		this._ensureRollbackState();

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

		// Optional hard caps
		this.rollbackMaxWatchCount =
			typeof opts?.maxWatchCount === "number"
				? opts.maxWatchCount
				: this.rollbackMaxWatchCount || 0;

		this.rollbackMaxRolledCount =
			typeof opts?.maxRolledCount === "number"
				? opts.maxRolledCount
				: this.rollbackMaxRolledCount || 0;

		// Dynamic prune slack (ms)
		this.rollbackPruneSlackMs =
			typeof opts?.rollbackPruneSlackMs === "number"
				? opts.rollbackPruneSlackMs
				: this.rollbackPruneSlackMs || DEFAULT_PRUNE_SLACK_MS;

		this._ensureRollbackState();

		if (this.rollbackEnabled) {
			this._ensureRollbackTimer("configureRollbackTracking");
		} else {
			clearTimeout(this._rollbackTimer);
			this._rollbackTimer = null;
		}
	};

	/**
	 * Add to watch on any accepted offer.
	 * - Idempotent via rb.watch[tradeId]
	 * - Skips if this tradeId is already marked as rolled back in rb.rolled
	 * - Watch lifetime: until = 7 full days from acceptedAt + next 07:00 (BST) / 08:00 (GMT) boundary
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

		this._ensureRollbackState();
		const rb = this.rollbackData;

		const tid = String(offer.tradeID);
		if (rb.rolled && rb.rolled[tid]) return; // already processed as rolled back
		if (rb.watch[tid]) return; // already watching

		const acceptedAtMs = offer.updated
			? offer.updated.getTime()
			: Date.now();
		const untilMs = computeSettlementDeadlineMs(acceptedAtMs);
		if (untilMs <= Date.now()) return; // already out of rollback window

		// Optional: enforce watch cap before adding a new one
		if (this.rollbackMaxWatchCount > 0) {
			const cur = Object.keys(rb.watch).length;
			if (cur >= this.rollbackMaxWatchCount) {
				_rbTrimWatch(rb, this.rollbackMaxWatchCount - 1);
			}
		}

		rb.watch[tid] = {
			offerId: offer.id,
			tradeId: tid,
			acceptedAt: acceptedAtMs,
			until: untilMs,
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

	// ---------- anchors & prune helpers ----------

	function _rbInsertAnchorSorted(rb, tSec, tradeId, gapSec, maxAnchors) {
		// Insert anchor sorted by time (asc). Avoid near-duplicates by checking neighbors.
		const anchors = rb.anchors || (rb.anchors = []);
		let idx = anchors.findIndex((a) => tSec < a.t);
		if (idx === -1) idx = anchors.length;

		const prev = anchors[idx - 1];
		const next = anchors[idx];
		if (prev && Math.abs(prev.t - tSec) < gapSec) return false;
		if (next && Math.abs(next.t - tSec) < gapSec) return false;

		anchors.splice(idx, 0, { t: tSec, id: String(tradeId) });

		// cap by size (drop oldest)
		if (anchors.length > maxAnchors) {
			anchors.splice(0, anchors.length - maxAnchors);
		}
		return true;
	}

	function _rbFindAnchorForTime(rb, targetSec) {
		// Return the smallest anchor with t >= targetSec (start scanning from just above the target time)
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
		const before = rb.anchors?.length || 0;
		if (!rb.anchors || before === 0) return false;
		rb.anchors = rb.anchors.filter((a) => a.t >= cutoffSec);
		return rb.anchors.length !== before;
	}

	function _rbPruneRolled(rb, cutoffSec, maxRolledCount) {
		const rolled = rb.rolled || {};
		let changed = false;

		// time-based prune
		for (const tid of Object.keys(rolled)) {
			const t = typeof rolled[tid] === "number" ? rolled[tid] : 0;
			if (t && t < cutoffSec) {
				delete rolled[tid];
				changed = true;
			}
		}

		// size-based trim (keep newest)
		if (maxRolledCount && maxRolledCount > 0) {
			const ids = Object.keys(rolled);
			if (ids.length > maxRolledCount) {
				const arr = ids
					.map((tid) => ({
						tid,
						t: typeof rolled[tid] === "number" ? rolled[tid] : 0,
					}))
					.sort((a, b) => a.t - b.t); // oldest first
				const removeN = ids.length - maxRolledCount;
				for (let i = 0; i < removeN; i++) {
					delete rolled[arr[i].tid];
					changed = true;
				}
			}
		}

		return changed;
	}

	function _rbPruneOfferToTradeOrphans(rb) {
		// Remove offerToTrade entries that don't point to an existing watch
		let changed = false;
		for (const offerId of Object.keys(rb.offerToTrade)) {
			const tid = rb.offerToTrade[offerId];
			if (!tid || !rb.watch[tid]) {
				delete rb.offerToTrade[offerId];
				changed = true;
			}
		}
		return changed;
	}

	function _rbTrimWatch(rb, maxCount) {
		if (!maxCount || maxCount <= 0) return false;
		const ids = Object.keys(rb.watch);
		if (ids.length <= maxCount) return false;

		const arr = ids
			.map((tid) => ({ tid, acceptedAt: rb.watch[tid].acceptedAt || 0 }))
			.sort((a, b) => a.acceptedAt - b.acceptedAt); // oldest first
		const removeN = ids.length - maxCount;
		let changed = false;
		for (let i = 0; i < removeN; i++) {
			const tid = arr[i].tid;
			const w = rb.watch[tid];
			if (
				w &&
				w.offerId != null &&
				Object.prototype.hasOwnProperty.call(rb.offerToTrade, w.offerId)
			) {
				delete rb.offerToTrade[w.offerId];
			}
			delete rb.watch[tid];
			changed = true;
		}
		return changed;
	}

	// ---------- core cycle ----------

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
		let persistNeeded = false;

		// GC expired watchers (per-trade dynamic deadline)
		{
			let changed = false;
			for (const tid in rb.watch) {
				if (!Object.prototype.hasOwnProperty.call(rb.watch, tid))
					continue;
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
			if (changed) {
				this._persistRollbackBlock();
				persistNeeded = false;
			} // already persisted
		}

		const allWatchIds = Object.keys(rb.watch);

		// Dynamic prune cutoff:
		// if there are active watches -> keep everything newer than (oldest until - slack)
		// else -> keep only the last `slack` window
		let pruneCutoffSec;
		const slackMs = this.rollbackPruneSlackMs || DEFAULT_PRUNE_SLACK_MS;
		if (allWatchIds.length > 0) {
			let oldestUntilMs = Infinity;
			for (const tid of allWatchIds) {
				const u = rb.watch[tid].until || now;
				if (u < oldestUntilMs) oldestUntilMs = u;
			}
			pruneCutoffSec = Math.floor(
				(Math.min(oldestUntilMs, now) - slackMs) / 1000
			);
		} else {
			pruneCutoffSec = Math.floor((now - slackMs) / 1000);
		}

		// Prune state/caps
		persistNeeded = _rbPruneOfferToTradeOrphans(rb) || persistNeeded;
		persistNeeded =
			_rbPruneRolled(rb, pruneCutoffSec, this.rollbackMaxRolledCount) ||
			persistNeeded;
		persistNeeded = _rbPruneAnchors(rb, pruneCutoffSec) || persistNeeded;
		persistNeeded =
			_rbTrimWatch(rb, this.rollbackMaxWatchCount) || persistNeeded;

		if (allWatchIds.length === 0) {
			this._rollbackBusy = false;
			if (persistNeeded) this._persistRollbackBlock();
			this.emit(
				"debug",
				`rollback: cycle end (no watchers) in ${
					Date.now() - cycleStart
				}ms`
			);
			this._startRollbackTimer(this.rollbackPollInterval * 2);
			return;
		}

		// 1) Fair subset selection using rotating offset (pickOffset)
		const maxChecks =
			this.rollbackMaxWatchChecksPerCycle || DEFAULT_MAX_WATCH_CHECKS;
		const sorted = allWatchIds
			.map((tid) => ({
				tid,
				acceptedAtSec: Math.floor(
					(rb.watch[tid].acceptedAt || now) / 1000
				),
			}))
			.sort((a, b) => b.acceptedAtSec - a.acceptedAtSec);

		const n = sorted.length;
		const k = Math.max(1, Math.min(maxChecks, n));
		const start = rb.pickOffset % n;
		const selected = [];
		for (let i = 0; i < k; i++) selected.push(sorted[(start + i) % n]);
		rb.pickOffset = (start + k) % n; // rotate next cycle
		persistNeeded = true; // pickOffset changed

		const watchSet = new Set(selected.map((x) => x.tid));
		const rangeMaxSec = selected[0].acceptedAtSec;
		const rangeMinSec = selected.reduce(
			(m, x) => Math.min(m, x.acceptedAtSec),
			selected[0].acceptedAtSec
		);
		const stopTimeSec = Math.max(
			pruneCutoffSec,
			rangeMinSec -
				(this.rollbackScanMarginSec || DEFAULT_SCAN_MARGIN_SEC)
		);

		// 2) Choose start anchor near rangeMaxSec; if none — start from head
		const pageSize =
			this.rollbackHistoryPageSize || DEFAULT_HISTORY_PAGE_SIZE;
		const timeBudgetMs =
			this.rollbackMaxScanMsPerCycle || DEFAULT_MAX_SCAN_MS;
		const gapSec = this.rollbackAnchorGapSec || DEFAULT_ANCHOR_GAP_SEC;
		const anchorsMax = this.rollbackAnchorsMax || DEFAULT_ANCHORS_MAX;

		const startAnchor = _rbFindAnchorForTime(rb, rangeMaxSec);
		const found = Object.create(null);
		let foundCount = 0;
		let anchorsTouched = false;

		let options = {
			max_trades: pageSize,
			include_failed: 1,
			include_total: 0,
			get_descriptions: 0,
			navigating_back: 0,
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
					return finalize();
				}

				const trades = Array.isArray(body.response.trades)
					? body.response.trades
					: [];
				if (trades.length === 0) {
					return finalize();
				}

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

				if (
					_rbInsertAnchorSorted(
						rb,
						lastTime,
						String(last.tradeid),
						gapSec,
						anchorsMax
					)
				) {
					anchorsTouched = true;
				}

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
						// mark as processed to avoid re-watching this tradeId on future polls
						rb.rolled[tid] =
							entry.time_mod || Math.floor(Date.now() / 1000);
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

			if (options.start_after_time && options.start_after_tradeid) {
				rb.cursor = {
					start_after_time: options.start_after_time,
					start_after_tradeid: options.start_after_tradeid,
				};
				persistNeeded = true;
			}

			if (anyChange || anchorsTouched || persistNeeded)
				this._persistRollbackBlock();

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
