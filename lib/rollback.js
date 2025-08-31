"use strict";

module.exports = function (TradeOfferManager) {
  const ROLLBACK_STATUS_CODE = 12; // "trade reversed" в истории
  const DEFAULT_ROLLBACK_POLL_INTERVAL = 60 * 1000; // 1 мин
  const DEFAULT_ROLLBACK_WINDOW_MS    = 8 * 24 * 60 * 60 * 1000; // 8 дней
  const DEFAULT_HISTORY_PAGE_SIZE     = 500;
  const DEFAULT_MAX_PAGES_PER_CYCLE   = 3;
  const CUT_OFF_BUFFER_SEC            = 10 * 60; // 10 минут
  const SEEN_TTL_MS                   = 24 * 60 * 60 * 1000; // 24 часа

  // Сколько страниц тратим на «голову» за цикл (остальное — в глубину)
  const HEAD_PAGES_PER_CYCLE          = 1; // можно сделать опцией при желании

  // ----------------- Служебные -----------------

  TradeOfferManager.prototype._ensureRollbackState = function () {
    if (!this.rollbackData || typeof this.rollbackData !== "object") {
      this.rollbackData = {};
    }
    const rb = this.rollbackData;
    if (!rb.watch || typeof rb.watch !== "object") rb.watch = {};               // tradeid -> entry
    if (!rb.offerToTrade || typeof rb.offerToTrade !== "object") rb.offerToTrade = {}; // offerid -> tradeid
    if (!rb.seen || typeof rb.seen !== "object") rb.seen = {};                  // tradeid -> seenAtMs (emit был)
    // курсор глубокой прокрутки
    if (!Object.prototype.hasOwnProperty.call(rb, "cursor")) rb.cursor = null;  // {start_after_time, start_after_tradeid} | null
  };

  TradeOfferManager.prototype._persistRollbackBlock = function () {
    this.emit("rollbackData", this.rollbackData);
  };

  TradeOfferManager.prototype._ensureRollbackTimer = function (reason) {
    if (!this.rollbackEnabled) return;
    this._ensureRollbackState();
    const rb = this.rollbackData;
    if (Object.keys(rb.watch).length === 0) return;
    this.emit("debug", `rollback: ensure timer (${reason || "unspecified"})`);
    this._startRollbackTimer(0);
  };

  /**
   * Полная замена rollbackData.
   * Поддерживает поля: {watch, offerToTrade, seen, cursor}
   */
  TradeOfferManager.prototype.setRollbackData = function (data, options) {
    const startTimer =
      options && typeof options.startTimer === "boolean" ? options.startTimer : true;

    this._ensureRollbackState();
    if (!data || typeof data !== "object") return;

    const rb = this.rollbackData;
    rb.watch        = data.watch && typeof data.watch === "object" ? data.watch : {};
    rb.offerToTrade = data.offerToTrade && typeof data.offerToTrade === "object" ? data.offerToTrade : {};
    rb.seen         = data.seen && typeof data.seen === "object" ? data.seen : rb.seen || {};
    rb.cursor       = data.cursor && typeof data.cursor === "object" ? data.cursor : null;

    this._persistRollbackBlock();
    if (startTimer) this._ensureRollbackTimer("setRollbackData");
  };

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

  // Наше исходящее предложение стало Accepted — добавляем в watch (идемпотентно)
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
    if (rb.seen[tradeIdStr]) return;   // уже эмитили откат
    if (rb.watch[tradeIdStr]) return;  // уже отслеживаем

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

    // ВНИМАНИЕ: курсор глубокой прокрутки НЕ трогаем — он про «coverage»,
    // а не про добавление в watch. Новые откаты поймает head‑скан.
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

  // --------- Основной цикл: HEAD + DEEP ---------
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

    // --- GC: watch по until, seen по TTL ---
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

    const watchSet = new Set(watchIds);

    // Жёсткая «нижняя» граница окна сканирования (в глубину)
    const hardCutoffSec =
      Math.max(1, Math.floor((now - (this.rollbackWindowMs || DEFAULT_ROLLBACK_WINDOW_MS)) / 1000) - CUT_OFF_BUFFER_SEC);

    const pageSize = this.rollbackHistoryPageSize || DEFAULT_HISTORY_PAGE_SIZE;
    const maxPages = this.rollbackMaxPagesPerCycle || DEFAULT_MAX_PAGES_PER_CYCLE;
    const headPages = Math.min(HEAD_PAGES_PER_CYCLE, maxPages);
    const deepPages = Math.max(0, maxPages - headPages);

    const found = Object.create(null);

    // Общая процедура обработки пачки trades
    const absorbBatch = (trades) => {
      for (let i = 0; i < trades.length; i++) {
        const t = trades[i];
        if (t.time_init && t.time_init < hardCutoffSec) {
          return { reachedCutoff: true }; // ниже окна
        }
        const tid = String(t.tradeid);
        if (watchSet.has(tid)) {
          found[tid] = t;
          if (Object.keys(found).length === watchSet.size) {
            return { done: true };
          }
        }
      }
      return { done: false, reachedCutoff: false };
    };

    // Универсальный пэйджер
    const pageLoop = (options, budgetPages, onFinish) => {
      let pagesUsed = 0;
      let reachedCutoff = false;
      let lastMore = false;
      const next = () => {
        this._apiCall("GET", "GetTradeHistory", 1, options, (err, body) => {
          if (err || !body || !body.response) {
            this.emit("debug", "GetTradeHistory error: " + (err ? err.message : "malformed"));
            return onFinish({ empty: true, pagesUsed, reachedCutoff, more: false, lastCursor: null });
          }

          const trades = Array.isArray(body.response.trades) ? body.response.trades : [];
          if (trades.length === 0) {
            return onFinish({ empty: true, pagesUsed, reachedCutoff, more: false, lastCursor: null });
          }

          const { done, reachedCutoff: hitCutoff } = absorbBatch(trades);
          reachedCutoff = reachedCutoff || hitCutoff;

          const last = trades[trades.length - 1];
          options.start_after_time = last.time_init;
          options.start_after_tradeid = String(last.tradeid);

          pagesUsed++;
          lastMore = !!body.response.more;

          if (done || reachedCutoff || !lastMore || pagesUsed >= budgetPages) {
            return onFinish({
              empty: false,
              pagesUsed,
              reachedCutoff,
              more: lastMore,
              lastCursor: { start_after_time: options.start_after_time, start_after_tradeid: options.start_after_tradeid },
            });
          } else {
            this.emit("debug", `GetTradeHistory paging; page=${pagesUsed}, cursor=${options.start_after_time}/${options.start_after_tradeid}`);
            next();
          }
        });
      };
      next();
    };

    // Фаза 1: HEAD‑скан
    const headOptions = {
      max_trades: pageSize,
      include_failed: 1,
      include_total: 0,
      get_descriptions: 0
      // без start_after_* — с головы
    };

    pageLoop(headOptions, headPages, (headRes) => {
      const headFoundAll = Object.keys(found).length === watchSet.size;
      const headStoppedByCutoff = !!headRes.reachedCutoff;

      // Если всё нашли на голове — заканчиваем
      if (headFoundAll) {
        return finalizeCycle({ deepUsed: false, newCursor: rb.cursor, reachedCutoff: headStoppedByCutoff });
      }

      // Фаза 2: DEEP‑скан — продолжение с сохранённого курсора
      if (deepPages <= 0 || !rb.cursor) {
        // глубину не делаем либо курсора нет — финиш
        return finalizeCycle({ deepUsed: false, newCursor: rb.cursor, reachedCutoff: headStoppedByCutoff });
      }

      const deepOptions = {
        max_trades: pageSize,
        include_failed: 1,
        include_total: 0,
        get_descriptions: 0,
        start_after_time: rb.cursor.start_after_time,
        start_after_tradeid: rb.cursor.start_after_tradeid
      };

      pageLoop(deepOptions, deepPages, (deepRes) => {
        // Новый курсор ставим только если: не дошли до границы окна И ещё есть куда идти
        const deepFinishedWindow = deepRes.reachedCutoff || !deepRes.more || deepRes.empty;
        const newCursor = deepFinishedWindow ? null : deepRes.lastCursor;
        return finalizeCycle({ deepUsed: true, newCursor, reachedCutoff: headStoppedByCutoff || deepRes.reachedCutoff });
      });
    });

    const finalizeCycle = ({ deepUsed, newCursor, reachedCutoff }) => {
      let anyChange = false;

      // Применяем found: либо откат (emit + удалить из watch), либо обновили lastSeenStatus
      for (const tid in found) {
        if (!Object.prototype.hasOwnProperty.call(found, tid)) continue;
        const entry = found[tid];
        const st = entry.status;

        if (st === ROLLBACK_STATUS_CODE) {
          const w = rb.watch[tid];
          if (w) {
            this.getOffer(w.offerId, (err, offer) => {
              const payloadOffer = err
                ? {
                    id: w.offerId,
                    tradeID: tid,
                    state: TradeOfferManager.ETradeOfferState.Accepted,
                    isOurOffer: true,
                  }
                : offer;
              this.emit("tradeRolledBack", payloadOffer, entry);
            });
            delete rb.watch[tid];
            delete rb.offerToTrade[w.offerId];
            rb.seen[tid] = Date.now();
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

      // Обновляем курсор глубокой прокрутки, если он реально использовался
      let cursorChanged = false;
      if (deepUsed) {
        const old = rb.cursor;
        const neu = newCursor;
        cursorChanged =
          (!!old && !!neu && (old.start_after_time !== neu.start_after_time || old.start_after_tradeid !== neu.start_after_tradeid)) ||
          (!old && !!neu) ||
          (!!old && !neu);
        rb.cursor = neu;
      }

      if (anyChange || cursorChanged) this._persistRollbackBlock();

      this._rollbackBusy = false;
      this.emit(
        "debug",
        `rollback: cycle end in ${Date.now() - cycleStart}ms` +
          (reachedCutoff ? " (cutoff)" : "") +
          (deepUsed ? ", deep" : ", head-only")
      );
      this._startRollbackTimer(this.rollbackPollInterval || DEFAULT_ROLLBACK_POLL_INTERVAL);
    };
  };
};
