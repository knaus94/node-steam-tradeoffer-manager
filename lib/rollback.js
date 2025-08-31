"use strict";

module.exports = function (TradeOfferManager) {
  const ROLLBACK_STATUS_CODE = 12; // status в истории, означающий откат
  const DEFAULT_ROLLBACK_POLL_INTERVAL = 60 * 1000; // 1 мин
  const DEFAULT_ROLLBACK_WINDOW_MS    = 8 * 24 * 60 * 60 * 1000; // 8 дней
  const DEFAULT_HISTORY_PAGE_SIZE     = 500;
  const DEFAULT_MAX_PAGES_PER_CYCLE   = 3;
  const CUT_OFF_BUFFER_SEC            = 10 * 60; // запас, чтобы не упереться ровно в acceptedAt
  const SEEN_TTL_MS                   = 24 * 60 * 60 * 1000; // держим метки уже-эмитнутых откатов 24 часа

  // --- Вспомогательное: гарантируем форму состояния ---
  TradeOfferManager.prototype._ensureRollbackState = function () {
    if (!this.rollbackData || typeof this.rollbackData !== "object") {
      this.rollbackData = {};
    }
    const rb = this.rollbackData;
    if (!rb.watch || typeof rb.watch !== "object") rb.watch = {};               // tradeid -> entry
    if (!rb.offerToTrade || typeof rb.offerToTrade !== "object") rb.offerToTrade = {}; // offerid -> tradeid
    if (!rb.seen || typeof rb.seen !== "object") rb.seen = {};                  // tradeid -> seenAtMs (emit был)
  };

  // Внешняя «сохранить состояние» (вдруг слушатель пишет на диск/редис)
  TradeOfferManager.prototype._persistRollbackBlock = function () {
    this.emit("rollbackData", this.rollbackData);
  };

  // Если трекинг включён и есть что смотреть — заводим таймер
  TradeOfferManager.prototype._ensureRollbackTimer = function (reason) {
    if (!this.rollbackEnabled) return;
    this._ensureRollbackState();
    const rb = this.rollbackData;
    if (Object.keys(rb.watch).length === 0) return;
    this.emit("debug", `rollback: ensure timer (${reason || "unspecified"})`);
    this._startRollbackTimer(0);
  };

  /**
   * Полная замена блока rollbackData
   * data: { watch?, offerToTrade?, seen? }
   */
  TradeOfferManager.prototype.setRollbackData = function (data, options) {
    const startTimer =
      options && typeof options.startTimer === "boolean" ? options.startTimer : true;

    this._ensureRollbackState();
    if (!data || typeof data !== "object") return;

    const rb = this.rollbackData;
    rb.watch        = data.watch && typeof data.watch === "object" ? data.watch : {};
    rb.offerToTrade = data.offerToTrade && typeof data.offerToTrade === "object" ? data.offerToTrade : {};
    // seen важен для дедупликации — если передали, используем, иначе оставляем текущий
    if (data.seen && typeof data.seen === "object") {
      rb.seen = data.seen;
    }

    this._persistRollbackBlock();
    if (startTimer) this._ensureRollbackTimer("setRollbackData");
  };

  // Конфигурирование трекинга
  TradeOfferManager.prototype.configureRollbackTracking = function (opts) {
    this.rollbackEnabled =
      opts && typeof opts.enabled !== "undefined" ? !!opts.enabled : this.rollbackEnabled;

    this.rollbackPollInterval =
      (opts && opts.pollInterval) || this.rollbackPollInterval || DEFAULT_ROLLBACK_POLL_INTERVAL;

    this.rollbackWindowMs =
      (opts && opts.windowMs) || this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS;

    this.rollbackMaxPagesPerCycle =
      (opts && opts.maxPagesPerCycle) || this.rollbackMaxPagesPerCycle || DEFAULT_MAX_PAGES_PER_CYCLE;

    this.rollbackHistoryPageSize =
      (opts && opts.pageSize) || this.rollbackHistoryPageSize || DEFAULT_HISTORY_PAGE_SIZE;

    this._ensureRollbackState();

    if (this.rollbackEnabled) {
      this._ensureRollbackTimer("configureRollbackTracking");
    } else {
      clearTimeout(this._rollbackTimer);
      this._rollbackTimer = null;
    }
  };

  // Наше исходящее предложение стало Accepted — ставим его на мониторинг отката
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
    if (rb.seen[tradeIdStr]) return;   // уже ловили откат этого трейда
    if (rb.watch[tradeIdStr]) return;  // уже смотрим

    const acceptedAtMs = offer.updated ? offer.updated.getTime() : Date.now();

    rb.watch[tradeIdStr] = {
      offerId: offer.id,
      tradeId: tradeIdStr,
      acceptedAt: acceptedAtMs,
      until: acceptedAtMs + (this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS),
      lastSeenStatus: TradeOfferManager.ETradeOfferState.Accepted,
      lastChecked: 0,
    };
    rb.offerToTrade[offer.id] = tradeIdStr;

    this._persistRollbackBlock();
    this._ensureRollbackTimer("offerAccepted");
  };

  // Таймерный запуск цикла
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

  // Основной цикл: всегда начинаем с «головы» истории, пагинируемся вниз максимум N страниц
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

    // --- GC просроченных watch и устаревших seen ---
    const now = Date.now();
    let changed = false;

    for (const tid in rb.watch) {
      if (!Object.prototype.hasOwnProperty.call(rb.watch, tid)) continue;
      const w = rb.watch[tid];
      if (w.until <= now) {
        if (w.offerId != null && Object.prototype.hasOwnProperty.call(rb.offerToTrade, w.offerId)) {
          delete rb.offerToTrade[w.offerId];
        }
        delete rb.watch[tid];
        changed = true;
      }
    }

    for (const tid in rb.seen) {
      if (!Object.prototype.hasOwnProperty.call(rb.seen, tid)) continue;
      if (rb.seen[tid] + SEEN_TTL_MS <= now) {
        delete rb.seen[tid];
        changed = true;
      }
    }

    if (changed) this._persistRollbackBlock();

    const watchIds = Object.keys(rb.watch);
    if (watchIds.length === 0) {
      this._rollbackBusy = false;
      this.emit("debug", `rollback: cycle end (no watchers) in ${Date.now() - cycleStart}ms`);
      this._startRollbackTimer(this.rollbackPollInterval * 2);
      return;
    }

    // cutoff = самый ранний acceptedAt среди watch минус буфер
    let minAcceptedAt = now;
    for (const tid of watchIds) {
      const w = rb.watch[tid];
      if (w && w.acceptedAt && w.acceptedAt < minAcceptedAt) {
        minAcceptedAt = w.acceptedAt;
      }
    }
    const cutoffSec = Math.max(1, Math.floor(minAcceptedAt / 1000) - CUT_OFF_BUFFER_SEC);

    const options = {
      max_trades: this.rollbackHistoryPageSize || DEFAULT_HISTORY_PAGE_SIZE,
      include_failed: 1,
      include_total: 0,
      get_descriptions: 0,
      // без start_after_*: каждый цикл начинаем сверху, пока не найдём всех нужных tradeid
    };

    let pages = 0;
    const watchSet = new Set(watchIds);
    const found = Object.create(null);
    let reachedCutoff = false;

    const request = () => {
      this._apiCall("GET", "GetTradeHistory", 1, options, (err, body) => {
        if (err || !body || !body.response) {
          this.emit("debug", "GetTradeHistory error: " + (err ? err.message : "malformed"));
          return finalize(true);
        }

        const trades = Array.isArray(body.response.trades) ? body.response.trades : [];
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
          this.emit("debug", `GetTradeHistory paging; page=${pages}, cursor=${options.start_after_time}/${options.start_after_tradeid}`);
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
            rb.seen[tid] = Date.now(); // пометили как уже-эмитнутый, чтобы не возвращался в watch
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

      if (anyChange) this._persistRollbackBlock();

      this._rollbackBusy = false;
      this.emit(
        "debug",
        `rollback: cycle end in ${Date.now() - cycleStart}ms (pages=${pages}${reachedCutoff ? ", cutoff" : ""})`
      );
      this._startRollbackTimer(this.rollbackPollInterval || DEFAULT_ROLLBACK_POLL_INTERVAL);
    };

    request();
  };
};
