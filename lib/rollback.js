"use strict";

module.exports = function (TradeOfferManager) {
	// --- Константы статусов ---
	const HISTORY_ROLLBACK_CODE = 12; // в GetTradeHistory status=12 для «откачан» (history-домен)
	const ETS = TradeOfferManager.ETradeStatus; // домен статусов GetTradeStatus

	// Множество «роллбэк»-статусов из GetTradeStatus
	const ROLLBACK_STATUS_SET = new Set([
		ETS.PartialSupportRollback, // 5
		ETS.FullSupportRollback, // 6
		ETS.SupportRollback_Selective, // 7
		ETS.EscrowRollback, // 11
	]);

	// Дефолты
	const DEFAULT_ROLLBACK_POLL_INTERVAL = 60 * 1000; // 1 мин
	const DEFAULT_ROLLBACK_WINDOW_MS = 8 * 24 * 60 * 60 * 1000; // 8 суток
	const DEFAULT_HISTORY_PAGE_SIZE = 200; // держим умеренно
	const DEFAULT_TOP_PAGES = 1; // верх истории, страниц за цикл
	const DEFAULT_STATUS_BUDGET = 50; // прямых статусов за цикл
	const DEFAULT_FIRST_CHECK_DELAY_MS = 60 * 1000; // 1 мин до первой проверки
	const DEFAULT_BACKOFF_SCHEDULE_MS = [
		60 * 1000, // 1m
		5 * 60 * 1000, // 5m
		15 * 60 * 1000, // 15m
		60 * 60 * 1000, // 1h
		4 * 60 * 60 * 1000, // 4h
		12 * 60 * 60 * 1000, // 12h
	];

	// ---- Вспомогательные ----

	function nowMs() {
		return Date.now();
	}

	// Безопасное приведение к числу ms
	function toMs(v, fallback) {
		return typeof v === "number" && v >= 0 ? v : fallback;
	}

	// Следующий интервал проверки по бэк-оффу
	function nextInterval(backoffSchedule, idx) {
		if (!Array.isArray(backoffSchedule) || backoffSchedule.length === 0) {
			return DEFAULT_BACKOFF_SCHEDULE_MS[
				Math.min(idx, DEFAULT_BACKOFF_SCHEDULE_MS.length - 1)
			];
		}
		return backoffSchedule[Math.min(idx, backoffSchedule.length - 1)];
	}

	// ---- Расширение состояния rollback ----

	TradeOfferManager.prototype._ensureRollbackState = function () {
		if (!this.rollbackData || typeof this.rollbackData !== "object") {
			this.rollbackData = {};
		}
		if (
			!this.rollbackData.watch ||
			typeof this.rollbackData.watch !== "object"
		) {
			this.rollbackData.watch = {};
		}
		if (
			!this.rollbackData.offerToTrade ||
			typeof this.rollbackData.offerToTrade !== "object"
		) {
			this.rollbackData.offerToTrade = {};
		}
		// cursor держим ради совместимости, но не используем
		if (
			!Object.prototype.hasOwnProperty.call(this.rollbackData, "cursor")
		) {
			this.rollbackData.cursor = null;
		}
		// Список «запечатанных» трэйдов: tid -> timestamp (до какого момента не ставить обратно)
		if (
			!this.rollbackData.sealed ||
			typeof this.rollbackData.sealed !== "object"
		) {
			this.rollbackData.sealed = {};
		}
	};

	TradeOfferManager.prototype._persistRollbackBlock = function () {
		this.emit("rollbackData", this.rollbackData);
	};

	// Локальный вызов GetTradeStatus
	TradeOfferManager.prototype._getTradeStatus = function (tradeId, cb) {
		this._apiCall(
			"GET",
			"GetTradeStatus",
			1,
			{
				tradeid: String(tradeId),
				get_descriptions: 0,
			},
			(err, body) => {
				if (err) return cb(err);
				if (!body || !body.response)
					return cb(new Error("Malformed GetTradeStatus response"));
				// типичный ответ: {response:{ trade: { tradeid, status, time_init, time_mod, ... } }}
				const trade =
					body.response.trade ||
					(Array.isArray(body.response.trades)
						? body.response.trades[0]
						: null);
				if (!trade || typeof trade.status === "undefined") {
					return cb(new Error("No trade object in GetTradeStatus"));
				}
				cb(null, trade);
			}
		);
	};

	// Идемпотентный запуск таймера
	TradeOfferManager.prototype._ensureRollbackTimer = function (reason) {
		if (!this.rollbackEnabled) return;
		this._ensureRollbackState();
		const rb = this.rollbackData;
		if (!rb || Object.keys(rb.watch).length === 0) return; // нечего слать
		this.emit(
			"debug",
			`rollback: ensure timer (${reason || "unspecified"})`
		);
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
			watch:
				data.watch && typeof data.watch === "object" ? data.watch : {},
			offerToTrade:
				data.offerToTrade && typeof data.offerToTrade === "object"
					? data.offerToTrade
					: {},
			cursor: Object.prototype.hasOwnProperty.call(data, "cursor")
				? data.cursor || null
				: null,
			// аккуратно подключим sealed, если пришёл извне старый формат
			sealed:
				data.sealed && typeof data.sealed === "object"
					? data.sealed
					: {},
		};

		this._persistRollbackBlock();
		if (startTimer) this._ensureRollbackTimer("setRollbackData");
	};

	// Конфигурация
	TradeOfferManager.prototype.configureRollbackTracking = function (opts) {
		this.rollbackEnabled =
			opts && typeof opts.enabled !== "undefined"
				? !!opts.enabled
				: this.rollbackEnabled;

		this.rollbackPollInterval = toMs(
			opts && opts.pollInterval,
			this.rollbackPollInterval || DEFAULT_ROLLBACK_POLL_INTERVAL
		);

		this.rollbackWindowMs = toMs(
			opts && opts.windowMs,
			this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS
		);

		// Верх истории (дёшево): сколько страниц и какой размер
		this.rollbackMaxPagesPerCycle =
			opts && typeof opts.maxPagesPerCycle === "number"
				? opts.maxPagesPerCycle
				: this.rollbackMaxPagesPerCycle || DEFAULT_TOP_PAGES;

		this.rollbackHistoryPageSize =
			opts && typeof opts.pageSize === "number"
				? opts.pageSize
				: this.rollbackHistoryPageSize || DEFAULT_HISTORY_PAGE_SIZE;

		// Точечные проверки статуса
		this.rollbackPerCycleStatusChecks =
			opts && typeof opts.perCycleStatusChecks === "number"
				? Math.max(0, opts.perCycleStatusChecks)
				: this.rollbackPerCycleStatusChecks || DEFAULT_STATUS_BUDGET;

		this.rollbackFirstCheckDelayMs = toMs(
			opts && opts.firstCheckDelayMs,
			this.rollbackFirstCheckDelayMs || DEFAULT_FIRST_CHECK_DELAY_MS
		);

		this.rollbackBackoffScheduleMs = Array.isArray(
			opts && opts.backoffScheduleMs
		)
			? opts.backoffScheduleMs
			: this.rollbackBackoffScheduleMs || DEFAULT_BACKOFF_SCHEDULE_MS;

		this._ensureRollbackState();

		if (this.rollbackEnabled) {
			this._ensureRollbackTimer("configureRollbackTracking");
		} else {
			clearTimeout(this._rollbackTimer);
			this._rollbackTimer = null;
		}
	};

	// Пришли в Accepted
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
		const now = nowMs();

		// Если трейд «запечатан» до конца окна — больше не следим и не пере-добавляем
		const sealUntil = rb.sealed[tid];
		if (typeof sealUntil === "number" && sealUntil > now) {
			return;
		}

		// уже в вотче?
		if (rb.watch[tid]) return;

		const acceptedAtMs = offer.updated ? offer.updated.getTime() : now;
		const until =
			acceptedAtMs +
			(this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS);

		rb.watch[tid] = {
			offerId: offer.id,
			tradeId: tid,
			acceptedAt: acceptedAtMs,
			until,
			lastSeenStatus: TradeOfferManager.ETradeOfferState.Accepted,
			lastChecked: 0,
			// новое:
			nextCheckAt:
				now +
				(this.rollbackFirstCheckDelayMs ||
					DEFAULT_FIRST_CHECK_DELAY_MS),
			backoffIdx: 0,
		};
		rb.offerToTrade[offer.id] = tid;

		// cursor нам больше не нужен — оставляем null
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
		const started = nowMs();

		this.emit("debug", "rollback: cycle start");

		const windowMs = this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS;

		// 1) GC: watch (по until) + sealed (протухшие)
		let mutated = false;
		const now = started;

		for (const tid in rb.watch) {
			if (!Object.prototype.hasOwnProperty.call(rb.watch, tid)) continue;
			const w = rb.watch[tid];
			if (w.until <= now) {
				// ставим печать, чтобы doPoll больше не клацал его обратно в окно
				rb.sealed[tid] = w.until; // до конца окна
				if (w.offerId && rb.offerToTrade[w.offerId]) {
					delete rb.offerToTrade[w.offerId];
				}
				delete rb.watch[tid];
				mutated = true;
			}
		}

		// чистим sealed, которые уже точно за окном (ещё +1 сутки форы)
		const SEALED_GRACE = 24 * 60 * 60 * 1000;
		for (const tid in rb.sealed) {
			if (!Object.prototype.hasOwnProperty.call(rb.sealed, tid)) continue;
			const until = rb.sealed[tid];
			if (typeof until === "number" && until + SEALED_GRACE <= now) {
				delete rb.sealed[tid];
				mutated = true;
			}
		}

		// 2) если watch пуст — откладываем следующий цикл и выходим
		const watchIds = Object.keys(rb.watch);
		if (watchIds.length === 0) {
			if (mutated) this._persistRollbackBlock();
			this._rollbackBusy = false;
			this.emit(
				"debug",
				`rollback: cycle end (no watchers) in ${nowMs() - started}ms`
			);
			this._startRollbackTimer(this.rollbackPollInterval * 2);
			return;
		}

		// 3) Лёгкий верх истории (0..N страниц), чтобы быстро ловить свежие роллбэки без бурения
		const doTopHistory = Math.max(
			0,
			this.rollbackMaxPagesPerCycle || DEFAULT_TOP_PAGES
		);
		const pageSize = Math.max(
			1,
			this.rollbackHistoryPageSize || DEFAULT_HISTORY_PAGE_SIZE
		);
		const watchSet = new Set(watchIds);
		let topHistoryPagesDone = 0;
		let anyChangeFromHistory = false;

		const scanTopHistory = (done) => {
			if (doTopHistory === 0) return done();

			let options = {
				max_trades: pageSize,
				include_failed: 1,
				include_total: 0,
				get_descriptions: 0,
			};
			let pages = 0;

			const step = () => {
				if (pages >= doTopHistory) return done();
				this._apiCall(
					"GET",
					"GetTradeHistory",
					1,
					options,
					(err, body) => {
						if (err || !body || !body.response) {
							this.emit(
								"debug",
								"GetTradeHistory(top) error: " +
									(err ? err.message : "malformed")
							);
							return done(); // не ломаем цикл
						}
						const trades = Array.isArray(body.response.trades)
							? body.response.trades
							: [];
						if (trades.length === 0) {
							return done();
						}

						for (let i = 0; i < trades.length; i++) {
							const t = trades[i];
							const tid = String(t.tradeid);
							if (
								watchSet.has(tid) &&
								t.status === HISTORY_ROLLBACK_CODE
							) {
								// детект роллбэка «сверху»
								const w = rb.watch[tid];
								if (w) {
									this.getOffer(w.offerId, (err2, offer) => {
										const payloadOffer = err2
											? {
													id: w.offerId,
													tradeID: tid,
													state: TradeOfferManager
														.ETradeOfferState
														.Accepted,
													isOurOffer: true,
											  }
											: offer;
										this.emit(
											"tradeRolledBack",
											payloadOffer,
											t
										);
									});
									// снимаем вотч и герметизируем
									delete rb.watch[tid];
									if (w.offerId)
										delete rb.offerToTrade[w.offerId];
									rb.sealed[tid] = w.until;
									anyChangeFromHistory = true;
								}
							}
						}

						// курсор дальше (только для топовых N страниц)
						const last = trades[trades.length - 1];
						options.start_after_time = last.time_init;
						options.start_after_tradeid = String(last.tradeid);

						pages++;
						topHistoryPagesDone = pages;

						if (body.response.more && pages < doTopHistory) {
							step();
						} else {
							done();
						}
					}
				);
			};

			step();
		};

		// 4) Точечные проверки статуса (GetTradeStatus) с бюджетом и бэк-оффом
		const checkStatuses = (done) => {
			const budget = Math.max(
				0,
				this.rollbackPerCycleStatusChecks || DEFAULT_STATUS_BUDGET
			);
			if (budget === 0) return done();

			// Собираем те, чей nextCheckAt <= now, и сортируем по сроку (старше — вперёд)
			const ready = [];
			for (const tid of watchIds) {
				const w = rb.watch[tid];
				if (!w) continue;
				if (!w.nextCheckAt) {
					w.nextCheckAt =
						now +
						(this.rollbackFirstCheckDelayMs ||
							DEFAULT_FIRST_CHECK_DELAY_MS);
					mutated = true;
				}
				if (w.nextCheckAt <= now) ready.push(w);
			}
			if (ready.length === 0) return done();

			ready.sort((a, b) => (a.nextCheckAt || 0) - (b.nextCheckAt || 0));
			const slice = ready.slice(0, budget);

			const iter = (idx) => {
				if (idx >= slice.length) return done();

				const w = slice[idx];
				this._getTradeStatus(w.tradeId, (err, trade) => {
					if (err || !trade) {
						// на ошибках — мягкий ретрай через минуту
						w.nextCheckAt = nowMs() + 60 * 1000;
						mutated = true;
						return iter(idx + 1);
					}

					const st = trade.status;
					if (ROLLBACK_STATUS_SET.has(st)) {
						// роллбэк
						this.getOffer(w.offerId, (err2, offer) => {
							const payloadOffer = err2
								? {
										id: w.offerId,
										tradeID: w.tradeId,
										state: TradeOfferManager
											.ETradeOfferState.Accepted,
										isOurOffer: true,
								  }
								: offer;
							this.emit("tradeRolledBack", payloadOffer, {
								tradeid: w.tradeId,
								status: st,
								time_init: trade.time_init,
								time_mod: trade.time_mod,
							});
						});
						// снимаем вотч и герметизируем
						delete rb.watch[w.tradeId];
						if (w.offerId) delete rb.offerToTrade[w.offerId];
						rb.sealed[w.tradeId] = w.until;
						mutated = true;
						return iter(idx + 1);
					}

					// не роллбэк: запланировать следующую проверку по бэк-оффу,
					// но хранить до конца окна (могут откатить позднее саппортом)
					w.lastSeenStatus = st;
					w.lastChecked = nowMs();
					w.backoffIdx = w.backoffIdx | 0;
					const delay = nextInterval(
						this.rollbackBackoffScheduleMs,
						w.backoffIdx
					);
					w.nextCheckAt = w.lastChecked + delay;
					if (w.backoffIdx < 1e6) w.backoffIdx++; // безопасно растёт, но не утекает
					mutated = true;

					iter(idx + 1);
				});
			};

			iter(0);
		};

		// Запускаем: top-history → direct-status → финал
		scanTopHistory(() => {
			checkStatuses(() => {
				if (mutated || anyChangeFromHistory)
					this._persistRollbackBlock();
				this._rollbackBusy = false;
				this.emit(
					"debug",
					`rollback: cycle end in ${
						nowMs() - started
					}ms (topPages=${topHistoryPagesDone}, checked=${Math.min(
						Object.keys(rb.watch).length,
						this.rollbackPerCycleStatusChecks ||
							DEFAULT_STATUS_BUDGET
					)})`
				);
				this._startRollbackTimer(
					this.rollbackPollInterval || DEFAULT_ROLLBACK_POLL_INTERVAL
				);
			});
		});
	};
};
