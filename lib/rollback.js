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

	// GC expired watches (by per-trade dynamic deadline)
	{
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
			persistNeeded = false;
		}
	}

	const allWatchIds = Object.keys(rb.watch);

	// Dynamic prune cutoff:
	// if there are active watches -> keep everything newer than (oldestUntil - slack)
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

	// Prune orphans/rolled/anchors; enforce caps
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
			`rollback: cycle end (no watchers) in ${Date.now() - cycleStart}ms`
		);
		this._startRollbackTimer(this.rollbackPollInterval * 2);
		return;
	}

	// Subset selection (rotating offset)...
	const maxChecks =
		this.rollbackMaxWatchChecksPerCycle || DEFAULT_MAX_WATCH_CHECKS;
	const sorted = allWatchIds
		.map((tid) => ({
			tid,
			acceptedAtSec: Math.floor((rb.watch[tid].acceptedAt || now) / 1000),
		}))
		.sort((a, b) => b.acceptedAtSec - a.acceptedAtSec);

	const n = sorted.length;
	const k = Math.max(1, Math.min(maxChecks, n));
	const start = rb.pickOffset % n;
	const selected = [];
	for (let i = 0; i < k; i++) selected.push(sorted[(start + i) % n]);
	rb.pickOffset = (start + k) % n;
	persistNeeded = true;

	const watchSet = new Set(selected.map((x) => x.tid));
	const rangeMaxSec = selected[0].acceptedAtSec;
	const rangeMinSec = selected.reduce(
		(m, x) => Math.min(m, x.acceptedAtSec),
		selected[0].acceptedAtSec
	);
	const stopTimeSec = Math.max(
		pruneCutoffSec,
		rangeMinSec - (this.rollbackScanMarginSec || DEFAULT_SCAN_MARGIN_SEC)
	);

	// Scan loop...
	const pageSize = this.rollbackHistoryPageSize || DEFAULT_HISTORY_PAGE_SIZE;
	const timeBudgetMs = this.rollbackMaxScanMsPerCycle || DEFAULT_MAX_SCAN_MS;
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
			if (trades.length === 0) return finalize();

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
									state: TradeOfferManager.ETradeOfferState
										.Accepted,
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
