"use strict";

module.exports = function (TradeOfferManager) {
	const ROLLBACK_STATUS_CODE = 12; // trade reversed in history
	const DEFAULT_ROLLBACK_POLL_INTERVAL = 60 * 1000; // 1 minute
	const DEFAULT_ROLLBACK_WINDOW_MS = 8 * 24 * 60 * 60 * 1000; // 8 days
	const DEFAULT_HISTORY_PAGE_SIZE = 500;
	const DEFAULT_MAX_PAGES_PER_CYCLE = 3;
	const CUT_OFF_BUFFER_SEC = 10 * 60; // seconds buffer

	// Допуск между offer.updated и history.time_init
	const HISTORY_SKEW_SEC = 3;

	function tradeIdLE(a, b) {
		const as = String(a);
		const bs = String(b);
		try {
			return BigInt(as) <= BigInt(bs);
		} catch {
			if (as.length !== bs.length) return as.length < bs.length;
			return as <= bs;
		}
	}

	// acceptedAtSec сравниваем с курсором из истории (time_init) с допуском
	function isBehindCursor(cursor, acceptedAtSec, tradeId) {
		if (
			!cursor ||
			!cursor.start_after_time ||
			!cursor.start_after_tradeid
		) {
			return false;
		}

		const s = cursor.start_after_time;

		// Сильно раньше курсора -> точно позади
		if (acceptedAtSec < s - HISTORY_SKEW_SEC) return true;
		// Сильно позже курсора -> точно впереди
		if (acceptedAtSec > s + HISTORY_SKEW_SEC) return false;

		// В пределах допуска: решаем по tradeid (cursor эксклюзивный, значит <= позади)
		return tradeIdLE(tradeId, cursor.start_after_tradeid);
	}

	TradeOfferManager.prototype._ensureRollbackState = function () {
		if (
			!this.rollbackData ||
			typeof this.rollbackData !== "object" ||
			!this.rollbackData.watch ||
			!this.rollbackData.offerToTrade ||
			!Object.prototype.hasOwnProperty.call(this.rollbackData, "cursor")
		) {
			this.rollbackData = { watch: {}, offerToTrade: {}, cursor: null };
		}
	};

	TradeOfferManager.prototype._persistRollbackBlock = function () {
		this.emit("rollbackData", this.rollbackData);
	};

	// Идемпотентный запуск таймера при включённом трекинге
	TradeOfferManager.prototype._ensureRollbackTimer = function (reason) {
		if (!this.rollbackEnabled) return;
		this._ensureRollbackState();
		const rb = this.rollbackData;
		if (!rb || !rb.watch || Object.keys(rb.watch).length === 0) return;
		this.emit("debug", `rollback: ensure timer (${reason || "unspecified"})`);
		this._startRollbackTimer(0);
	};

	/**
	 * Полная замена rollbackData
	 */
	TradeOfferManager.prototype.setRollbackData = function (data, options) {
		const startTimer =
			options && typeof options.startTimer === "boolean"
				? options.startTimer
				: true;

		this._ensureRollbackState();
		if (!data || typeof data !== "object") return;

		this.rollbackData = {
			watch: data.watch && typeof data.watch === "object" ? data.watch : {},
			offerToTrade:
				data.offerToTrade && typeof data.offerToTrade === "object"
					? data.offerToTrade
					: {},
			cursor: Object.prototype.hasOwnProperty.call(data, "cursor")
				? data.cursor || null
				: null,
		};

		this._persistRollbackBlock();
		if (startTimer) this._ensureRollbackTimer("setRollbackData");
	};

	// Включение/отключение трекинга
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
			this._ensureRollbackTimer("configureRollbackTracking");
		} else {
			clearTimeout(this._rollbackTimer);
			this._rollbackTimer = null;
		}
	};

	// Наше исходящее предложение достигло Accepted
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
		if (rb.watch[tradeIdStr]) return; // идемпотентность

		const acceptedAtMs = offer.updated ? offer.updated.getTime() : Date.now();
		const acceptedAtSec = Math.floor(acceptedAtMs / 1000);

		// Если уже позади курсора (с учетом допуска) — НЕ добавляем в watch
		if (isBehindCursor(rb.cursor, acceptedAtSec, tradeIdStr)) {
			return;
		}

		// Новый вотч
		rb.watch[tradeIdStr] = {
			offerId: offer.id,
			tradeId: tradeIdStr,
			acceptedAt: acceptedAtMs,
			until:
				acceptedAtMs + (this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS),
			lastSeenStatus: TradeOfferManager.ETradeOfferState.Accepted,
			lastChecked: 0,
		};
		rb.offerToTrade[offer.id] = tradeIdStr;

		// Новый вотч "новее" прогресса — сбрасываем курсор, чтобы следующий цикл стартовал сверху
		rb.cursor = null;

		this._persistRollbackBlock();
		this._ensureRollbackTimer("offerAccepted");
	};

	// Таймер
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

	// Основной цикл
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

		// GC просроченных вотчей
		const now = Date.now();
		let gcChanged = false;
		for (const tid in rb.watch) {
			if (!Object.prototype.hasOwnProperty.call(rb.watch, tid)) continue;
			const w = rb.watch[tid];
			if (w.until <= now) {
				if (
					w.offerId != null &&
					Object.prototype.hasOwnProperty.call(rb.offerToTrade, w.offerId)
				) {
					delete rb.offerToTrade[w.offerId];
				}
				delete rb.watch[tid];
				gcChanged = true;
			}
		}
		if (gcChanged) this._persistRollbackBlock();

		const watchIds = Object.keys(rb.watch);
		if (watchIds.length === 0) {
			this._rollbackBusy = false;
			this.emit(
				"debug",
				`rollback: cycle end (no watchers) in ${Date.now() - cycleStart}ms`
			);
			this._startRollbackTimer(this.rollbackPollInterval * 2);
			return;
		}

		const watchSet = new Set(watchIds);
		const cutoffSec =
			Math.floor(
				(now - (this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS)) / 1000
			) - CUT_OFF_BUFFER_SEC;

		let options = {
			max_trades: this.rollbackHistoryPageSize || DEFAULT_HISTORY_PAGE_SIZE,
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
						"GetTradeHistory error: " + (err ? err.message : "malformed")
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
					pages < (this.rollbackMaxPagesPerCycle || DEFAULT_MAX_PAGES_PER_CYCLE);

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
										state: TradeOfferManager.ETradeOfferState.Accepted,
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

			// Обновляем курсор (сохраняем прогресс сканирования)
			let newCursor = null;
			if (!empty && !reachedCutoff) {
				newCursor = {
					start_after_time: options.start_after_time,
					start_after_tradeid: options.start_after_tradeid,
				};
			}
			const oldCursor = rb.cursor;
			const cursorChanged =
				(oldCursor &&
					newCursor &&
					(oldCursor.start_after_time !== newCursor.start_after_time ||
						oldCursor.start_after_tradeid !== newCursor.start_after_tradeid)) ||
				(!oldCursor && !!newCursor) ||
				(!!oldCursor && !newCursor);

			rb.cursor = newCursor;

			if (anyChange || cursorChanged) this._persistRollbackBlock();

			this._rollbackBusy = false;
			this.emit(
				"debug",
				`rollback: cycle end in ${Date.now() - cycleStart}ms (pages=${pages}${
					reachedCutoff ? ", cutoff" : ""
				})`
			);
			this._startRollbackTimer(
				this.rollbackPollInterval || DEFAULT_ROLLBACK_POLL_INTERVAL
			);
		};

		request();
	};
};
