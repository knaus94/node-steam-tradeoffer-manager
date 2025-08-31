"use strict";

module.exports = function (TradeOfferManager) {
	const ROLLBACK_STATUS_CODE = 12; // trade reversed in history
	const DEFAULT_ROLLBACK_POLL_INTERVAL = 60 * 1000; // 1 minute
	const DEFAULT_ROLLBACK_WINDOW_MS = 8 * 24 * 60 * 60 * 1000; // 8 days
	const DEFAULT_HISTORY_PAGE_SIZE = 500;
	const DEFAULT_MAX_PAGES_PER_CYCLE = 3;
	const CUT_OFF_BUFFER_SEC = 10 * 60; // seconds buffer

	// // Safe compare "a <= b" for tradeid (string/number)
	function tradeIdLE(a, b) {
		const as = String(a);
		const bs = String(b);
		try {
			// Prefer numeric compare if possible
			return BigInt(as) <= BigInt(bs);
		} catch {
			// Fallback: by length then lexicographically
			if (as.length !== bs.length) return as.length < bs.length;
			return as <= bs;
		}
	}

	// // Whether a trade (acceptedAtSec, tradeId) is already at-or-before the cursor
	function isBehindCursor(cursor, acceptedAtSec, tradeId) {
		if (
			!cursor ||
			!cursor.start_after_time ||
			!cursor.start_after_tradeid
		) {
			return false;
		}
		if (acceptedAtSec < cursor.start_after_time) return true;
		if (acceptedAtSec > cursor.start_after_time) return false;
		// Same second: use tradeid tie-breaker (cursor is exclusive, so <= means behind)
		return tradeIdLE(tradeId, cursor.start_after_tradeid);
	}

	// Ensure rollback state holder
	TradeOfferManager.prototype._ensureRollbackState = function () {
		// // Shape guard to avoid partial objects coming from options.rollbackData or disk
		if (
			!this.rollbackData ||
			typeof this.rollbackData !== "object" ||
			!this.rollbackData.watch ||
			!this.rollbackData.offerToTrade ||
			!Object.prototype.hasOwnProperty.call(this.rollbackData, "cursor")
		) {
			this.rollbackData = {
				watch: {},
				offerToTrade: {},
				cursor: null,
			};
		}
	};

	// Emit rollback block changes
	TradeOfferManager.prototype._persistRollbackBlock = function () {
		this.emit("rollbackData", this.rollbackData);
	};

	/**
	 * Replace current rollback block with provided data.
	 * Use this after you load data asynchronously elsewhere.
	 * @param {object} data - {watch, offerToTrade, cursor}
	 * @param {object} [options]
	 * @param {boolean} [options.startTimer=true] - start rollback timer if tracking is enabled
	 */
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
		};

		this._persistRollbackBlock();
		if (this.rollbackEnabled && startTimer) this._startRollbackTimer(0);
	};

	// Configure (start/stop) rollback tracking
	TradeOfferManager.prototype.configureRollbackTracking = function (opts) {
		this.rollbackEnabled =
			opts && typeof opts.enabled !== "undefined"
				? !!opts.enabled
				: this.rollbackEnabled;
		this.rollbackPollInterval =
			(opts && opts.pollInterval) ||
			this.rollbackPollInterval ||
			DEFAULT_ROLLBACK_POLL_INTERVAL;
		this.rollbackWindowMs =
			(opts && opts.windowMs) ||
			this.rollbackWindowMs ||
			DEFAULT_ROLLBACK_WINDOW_MS;
		this.rollbackMaxPagesPerCycle =
			(opts && opts.maxPagesPerCycle) ||
			this.rollbackMaxPagesPerCycle ||
			DEFAULT_MAX_PAGES_PER_CYCLE;
		this.rollbackHistoryPageSize =
			(opts && opts.pageSize) ||
			this.rollbackHistoryPageSize ||
			DEFAULT_HISTORY_PAGE_SIZE;

		this._ensureRollbackState();

		if (this.rollbackEnabled) {
			this._startRollbackTimer(0);
		} else {
			clearTimeout(this._rollbackTimer);
			this._rollbackTimer = null;
		}
	};

	// Called when our outgoing offer is in Accepted state
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

		const tradeIdStr = String(offer.tradeID);
		if (rb.watch[tradeIdStr]) return; // idempotent

		const acceptedAtMs = offer.updated
			? offer.updated.getTime()
			: Date.now();
		const acceptedAtSec = Math.floor(acceptedAtMs / 1000);

		// // If this trade is already at-or-before the cursor, we have already scanned past it.
		// // Do NOT add it to watch and do NOT touch the cursor.
		if (isBehindCursor(rb.cursor, acceptedAtSec, tradeIdStr)) {
			return;
		}

		// // Fresh watch for a trade that is not behind the cursor
		rb.watch[tradeIdStr] = {
			offerId: offer.id,
			tradeId: tradeIdStr,
			acceptedAt: acceptedAtMs,
			until:
				acceptedAtMs +
				(this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS),
			lastSeenStatus: TradeOfferManager.ETradeOfferState.Accepted,
			lastChecked: 0,
		};

		rb.offerToTrade[offer.id] = tradeIdStr;

		// // IMPORTANT: do NOT reset cursor here. We keep scan progress.
		this._persistRollbackBlock();
		if (this.rollbackEnabled) this._startRollbackTimer(0);
	};

	// Timer management
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

	// Main polling cycle over GetTradeHistory
	TradeOfferManager.prototype._runRollbackCycle = function () {
		if (!this.rollbackEnabled) return;

		if (this._rollbackBusy) {
			this._startRollbackTimer(this.rollbackPollInterval);
			return;
		}
		this._rollbackBusy = true;

		this._ensureRollbackState();
		const rb = this.rollbackData;

		// GC expired watchers
		const now = Date.now();
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
		if (changed) {
			this._persistRollbackBlock();
		}

		const watchIds = Object.keys(rb.watch);
		if (watchIds.length === 0) {
			this._rollbackBusy = false;
			this._startRollbackTimer(this.rollbackPollInterval * 2);
			return;
		}

		const watchSet = new Set(watchIds);
		const cutoffSec =
			Math.floor(
				(now - (this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS)) /
					1000
			) - CUT_OFF_BUFFER_SEC;

		let options = {
			max_trades:
				this.rollbackHistoryPageSize || DEFAULT_HISTORY_PAGE_SIZE,
			include_failed: 1,
			include_total: 0,
			get_descriptions: 0,
		};

		if (
			rb.cursor &&
			rb.cursor.start_after_time &&
			rb.cursor.start_after_tradeid
		) {
			options.start_after_time = rb.cursor.start_after_time;
			options.start_after_tradeid = rb.cursor.start_after_tradeid;
		}

		let pages = 0;
		let found = Object.create(null);
		let reachedCutoff = false;

		const request = () => {
			this._apiCall("GET", "GetTradeHistory", 1, options, (err, body) => {
				if (err || !body || !body.response) {
					this.emit(
						"debug",
						"GetTradeHistory error: " +
							(err ? err.message : "malformed")
					);
					return finalize(true);
				}

				const trades = Array.isArray(body.response.trades)
					? body.response.trades
					: [];
				if (trades.length === 0) {
					return finalize(true);
				}

				for (let i = 0; i < trades.length; i++) {
					const t = trades[i];
					if (t.time_init && t.time_init < cutoffSec) {
						reachedCutoff = true;
						break;
					}
					const tid = String(t.tradeid);
					if (watchSet.has(tid)) {
						found[tid] = t;
					}
				}

				const last = trades[trades.length - 1];
				options.start_after_time = last.time_init;
				options.start_after_tradeid = String(last.tradeid);

				pages++;
				const needMore =
					Object.keys(found).length < watchSet.size &&
					!reachedCutoff &&
					body.response.more &&
					pages <
						(this.rollbackMaxPagesPerCycle ||
							DEFAULT_MAX_PAGES_PER_CYCLE);

				if (needMore) {
					this.emit(
						"debug",
						`GetTradeHistory paging; page=${pages}, cursor=${options.start_after_time}/${options.start_after_tradeid}`
					);
					request();
				} else {
					finalize(false);
				}
			});
		};

		const finalize = (empty) => {
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
						delete rb.offerToTrade[watch.offerId];
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

			// // Update scan cursor: keep progress only when we actually paged
			if (!empty && !reachedCutoff) {
				rb.cursor = {
					start_after_time: options.start_after_time,
					start_after_tradeid: options.start_after_tradeid,
				};
			}

			if (anyChange) this._persistRollbackBlock();

			this._rollbackBusy = false;
			this._startRollbackTimer(
				this.rollbackPollInterval || DEFAULT_ROLLBACK_POLL_INTERVAL
			);
		};

		const start = Date.now();
		this.emit(
			"debug",
			`Starting rollback cycle. Start after time: ${options.start_after_time}, Start after tradeid: ${options.start_after_tradeid}`
		);
		request();
		this.emit(
			"debug",
			`Ending rollback cycle. Start after time: ${
				options.start_after_time
			}, Start after tradeid: ${options.start_after_tradeid} in ${
				Date.now() - start
			}ms`
		);
	};
};
