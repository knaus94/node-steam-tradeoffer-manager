"use strict";

const deepEqual = require('fast-deep-equal');

const TradeOfferManager = require('./index.js');

const ETradeOfferState = TradeOfferManager.ETradeOfferState;
const EOfferFilter = TradeOfferManager.EOfferFilter;
const EConfirmationMethod = TradeOfferManager.EConfirmationMethod;

/*
 * pollData is an object which has the following structure:
 *  - `offersSince` is the STANDARD unix time (Math.floor(Date.now() / 1000)) of the last known offer change
 *  - `sent` is an object whose keys are offer IDs for known offers we've sent and whose values are the last known states of those offers
 *  - `received` is the same as `sent`, for offers we've received
 *  - `offerData` is an object whose keys are offer IDs. Values are objects mapping arbitrary keys to arbitrary values.
 *    Some keys are reserved for offer-specific options. These are:
 *      - `cancelTime` - The time, in milliseconds, after which the offer should be canceled automatically. Defaults to the TradeOfferManager's set cancelTime.
 *      - `pendingCancelTime` - Ditto `cancelTime`, except only for offers which are CreatedNeedsConfirmation.
 *
 *  - `offerMeta` (added): lightweight meta per offer:
 *      { [offerId]: { lastUpdatedSec: number, lastState: number } }
 */

TradeOfferManager.prototype.doPoll = function(doFullUpdate) {
	if (!this.apiKey && !this.accessToken) {
		// In case a race condition causes this to be called after we've shutdown or before we have an api key or access token
		return;
	}

	const timeSinceLastPoll = Date.now() - this._lastPoll;

	if (timeSinceLastPoll < this.minimumPollInterval) {
		// We last polled less than a second ago... we shouldn't spam the API
		// Reset the timer to poll minimumPollInterval after the last one
		this._resetPollTimer(this.minimumPollInterval - timeSinceLastPoll);
		return;
	}

	this._lastPoll = Date.now();
	clearTimeout(this._pollTimer);

	// ---------- Decide full update vs incremental ----------
	const hasLiveState = (st) =>
		st === ETradeOfferState.Active ||
		st === ETradeOfferState.InEscrow ||
		st === ETradeOfferState.CreatedNeedsConfirmation;

	const mapHasLive = (map) => {
		if (!map) return false;
		for (const id in map) {
			if (hasLiveState(map[id])) return true;
		}
		return false;
	};

	const hasLiveOffers = () => {
		return mapHasLive(this.pollData.sent) || mapHasLive(this.pollData.received);
	};

	const rollbackWatchCount = (() => {
		try {
			const rb = this.rollbackData;
			if (rb && rb.watch && typeof rb.watch === 'object') {
				return Object.keys(rb.watch).length;
			}
		} catch (_) {}
		return 0;
	})();

	let offersSince = 0;
	if (this.pollData.offersSince) {
		// It looks like sometimes Steam can be dumb and backdate a modified offer. We need to handle this.
		// Let's add a 30-minute buffer.
		offersSince = this.pollData.offersSince - 1800;
	}

	let fullUpdate = false;
	const eligibleByInterval = (Date.now() - this._lastPollFullUpdate) >= this.pollFullUpdateInterval;

	// Skip periodic full update when idle (no live offers and no rollback watchers)
	const idle = !hasLiveOffers() && rollbackWatchCount === 0;

	if (doFullUpdate) {
		fullUpdate = true;
	} else if (eligibleByInterval && !idle) {
		fullUpdate = true;
	}

	if (fullUpdate) {
		this._lastPollFullUpdate = Date.now();
		offersSince = 1;
	}

	this.emit('debug', 'Doing trade offer poll since ' + offersSince + (fullUpdate ? ' (full update)' : ''));
	var requestStart = Date.now();

	this.getOffers(fullUpdate ? EOfferFilter.All : EOfferFilter.ActiveOnly, new Date(offersSince * 1000), (err, sent, received) => {
		if (err) {
			this.emit('debug', "Error getting trade offers for poll: " + err.message);
			this.emit('pollFailure', err);
			this._resetPollTimer();
			return;
		}

		this.emit('debug', 'Trade offer poll succeeded in ' + (Date.now() - requestStart) + ' ms');

		var origPollData = JSON.parse(JSON.stringify(this.pollData)); // deep clone

		var timestamps = this.pollData.timestamps || {};
		var sentStates = this.pollData.sent || {};
		// // Meta bucket: track last state-change time per offer to drive safe pruning later
		var meta = this.pollData.offerMeta || {};

		var hasGlitchedOffer = false;

		// --------------------------
		// Process SENT offers
		// --------------------------
		sent.forEach((offer) => {
			// // Use updated time if available; it's a Date object
			var updatedSec = Math.floor(offer.updated.getTime() / 1000);

			if (!sentStates[offer.id]) {
				// We sent this offer, but we have no record of it! Good job Steam
				// Apparently offers can appear in the API before the send() call has returned, so we'll need to add a delay
				// Only emit the unknownOfferSent event if currently there's no offers that await a response in .send
				if (!this._pendingOfferSendResponses) {
					if (offer.fromRealTimeTrade) {
						// This is a real-time trade offer.
						if (offer.state == ETradeOfferState.CreatedNeedsConfirmation || (offer.state == ETradeOfferState.Active && offer.confirmationMethod != EConfirmationMethod.None)) {
							// we need to confirm this
							this.emit('realTimeTradeConfirmationRequired', offer);
						} else if (offer.state == ETradeOfferState.Accepted) {
							// both parties confirmed, trade complete
							this.emit('realTimeTradeCompleted', offer);
						}
					}

					this.emit('unknownOfferSent', offer);
					sentStates[offer.id] = offer.state;
					timestamps[offer.id] = offer.created.getTime() / 1000;

					// // Track meta for new offer
					meta[offer.id] = { lastUpdatedSec: updatedSec, lastState: offer.state };
				}
			} else if (offer.state != sentStates[offer.id]) {
				if (!offer.isGlitched()) {
					// We sent this offer, and it has now changed state
					if (offer.fromRealTimeTrade && offer.state == ETradeOfferState.Accepted) {
						this.emit('realTimeTradeCompleted', offer);
					}

					this.emit('sentOfferChanged', offer, sentStates[offer.id]);
					sentStates[offer.id] = offer.state;
					timestamps[offer.id] = offer.created.getTime() / 1000;

					// // Update meta on state change
					meta[offer.id] = { lastUpdatedSec: updatedSec, lastState: offer.state };
				} else {
					hasGlitchedOffer = true;
					var countWithoutName = !this._language ? 0 : offer.itemsToGive.concat(offer.itemsToReceive).filter(function(item) { return !item.name; }).length;
					this.emit('debug', "Not emitting sentOfferChanged for " + offer.id + " right now because it's glitched (" +
						offer.itemsToGive.length + " to give, " + offer.itemsToReceive.length + " to receive, " + countWithoutName + " without name)");
				}
			}

			if (offer.state == ETradeOfferState.Active) {
				// The offer is still Active, and we sent it. See if it's time to cancel it automatically.
				var cancelTime = this.cancelTime;

				// Check if this offer has a custom cancelTime
				var customCancelTime = offer.data('cancelTime');
				if (typeof customCancelTime !== 'undefined') {
					cancelTime = customCancelTime;
				}

				if (cancelTime && (Date.now() - offer.updated.getTime() >= cancelTime)) {
					offer.cancel((err) => {
						if (!err) {
							this.emit('sentOfferCanceled', offer, 'cancelTime');
						} else {
							this.emit('debug', "Can't auto-cancel offer #" + offer.id + ": " + err.message);
						}
					});
				}
			}

			if (offer.state == ETradeOfferState.CreatedNeedsConfirmation && this.pendingCancelTime) {
				// The offer needs to be confirmed to be sent. Let's see if the maximum time has elapsed before we cancel it.
				var pendingCancelTime = this.pendingCancelTime;

				var customPendingCancelTime = offer.data('pendingCancelTime');
				if (typeof customPendingCancelTime !== 'undefined') {
					pendingCancelTime = customPendingCancelTime;
				}

				if (pendingCancelTime && (Date.now() - offer.created.getTime() >= pendingCancelTime)) {
					offer.cancel((err) => {
						if (!err) {
							this.emit('sentPendingOfferCanceled', offer);
						} else {
							this.emit('debug', "Can't auto-canceling pending-confirmation offer #" + offer.id + ": " + err.message);
						}
					});
				}
			}

			if (offer.isOurOffer && offer.state == ETradeOfferState.Accepted && this._onOfferAccepted) {
				this._onOfferAccepted(offer); // // idempotent inside
			}
		});

		if (this.cancelOfferCount) {
			var sentActive = sent.filter(offer => offer.state == ETradeOfferState.Active);

			if (sentActive.length >= this.cancelOfferCount) {
				// We have too many offers out. Let's cancel the oldest.
				// Use updated since that reflects when it was confirmed, if necessary.
				var oldest = sentActive[0];
				for (var i = 1; i < sentActive.length; i++) {
					if (sentActive[i].updated.getTime() < oldest.updated.getTime()) {
						oldest = sentActive[i];
					}
				}

				// Make sure it's old enough
				if (Date.now() - oldest.updated.getTime() >= this.cancelOfferCountMinAge) {
					oldest.cancel((err) => {
						if (!err) {
							this.emit('sentOfferCanceled', oldest, 'cancelOfferCount');
						}
					});
				}
			}
		}

		this.pollData.sent = sentStates;

		// --------------------------
		// Process RECEIVED offers
		// --------------------------
		var receivedStates = this.pollData.received || {};
		received.forEach((offer) => {
			var updatedSec = Math.floor(offer.updated.getTime() / 1000);

			if (offer.isGlitched()) {
				hasGlitchedOffer = true;
				return;
			}

			if (offer.fromRealTimeTrade) {
				// This is a real-time trade offer
				if (!receivedStates[offer.id] && (offer.state == ETradeOfferState.CreatedNeedsConfirmation || (offer.state == ETradeOfferState.Active && offer.confirmationMethod != EConfirmationMethod.None))) {
					this.emit('realTimeTradeConfirmationRequired', offer);
				} else if (offer.state == ETradeOfferState.Accepted && (!receivedStates[offer.id] || (receivedStates[offer.id] != offer.state))) {
					this.emit('realTimeTradeCompleted', offer);
				}
			}

			if (!receivedStates[offer.id] && offer.state == ETradeOfferState.Active) {
				this.emit('newOffer', offer);
				// // Track meta for new incoming Active offer
				meta[offer.id] = { lastUpdatedSec: updatedSec, lastState: offer.state };
			} else if (receivedStates[offer.id] && offer.state != receivedStates[offer.id]) {
				this.emit('receivedOfferChanged', offer, receivedStates[offer.id]);
				// // Update meta on state change
				meta[offer.id] = { lastUpdatedSec: updatedSec, lastState: offer.state };
			}

			receivedStates[offer.id] = offer.state;
			timestamps[offer.id] = offer.created.getTime() / 1000;
		});

		this.pollData.received = receivedStates;
		this.pollData.timestamps = timestamps;
		this.pollData.offerMeta = meta;

		// --------------------------
		// Compute latest update watermark
		// --------------------------
		if (!hasGlitchedOffer) {
			var latest = this.pollData.offersSince || 0;
			sent.concat(received).forEach((offer) => {
				var updated = Math.floor(offer.updated.getTime() / 1000);
				if (updated > latest) {
					latest = updated;
				}
			});

			this.pollData.offersSince = latest;
		}

		// --------------------------
		// Prune finished offers from pollData (safe retention)
		// --------------------------
		(function prunePollData(manager) {
			// // Steam sometimes backdates timestamps; give a grace period
			const GRACE_SEC = 45 * 60;

			// // Retention windows (seconds)
			const RETENTION = {
				ACCEPTED: 24 * 3600,           // keep Accepted for 24h; rollbacks tracked separately
				FINAL: 6 * 3600,               // Declined/Canceled/Expired/InvalidItems/CancelledBySecondFactor
			};

			const nowSec = Math.floor(Date.now() / 1000);

			const KEEP_STATES = new Set([
				ETradeOfferState.Active,
				ETradeOfferState.InEscrow,
				ETradeOfferState.CreatedNeedsConfirmation
			]);

			const isFinal = (st) =>
				st === ETradeOfferState.Declined ||
				st === ETradeOfferState.Canceled ||
				st === ETradeOfferState.Expired ||
				st === ETradeOfferState.InvalidItems ||
				st === ETradeOfferState.CanceledBySecondFactor;

			const timestamps = manager.pollData.timestamps || {};
			const meta = manager.pollData.offerMeta || {};
			const sentMap = manager.pollData.sent || {};
			const recvMap = manager.pollData.received || {};

			// // Helper to drop an offer id from maps
			const drop = (id) => {
				delete sentMap[id];
				delete recvMap[id];
				delete timestamps[id];
				delete meta[id];
			};

			// // Check both maps
			const considerMap = (map) => {
				for (const id in map) {
					const st = map[id];
					if (KEEP_STATES.has(st)) {
						continue; // never prune live offers
					}

					// // Prefer lastUpdatedSec, fallback to timestamps (createdSec)
					const m = meta[id];
					const lastUpdated = m && m.lastUpdatedSec ? m.lastUpdatedSec : (timestamps[id] || 0);
					if (!lastUpdated) continue;

					const age = nowSec - lastUpdated;
					if (age < GRACE_SEC) continue; // too fresh

					if (st === ETradeOfferState.Accepted) {
						if (age > RETENTION.ACCEPTED) {
							drop(id);
						}
						continue;
					}

					if (isFinal(st)) {
						if (age > RETENTION.FINAL) {
							drop(id);
						}
						continue;
					}

					// // Other rare states: keep
				}
			};

			considerMap(sentMap);
			considerMap(recvMap);

			// // Optional size cap on non-live entries (avoid unbounded growth)
			const capNonActive = (map) => {
				const items = [];
				for (const id in map) {
					const st = map[id];
					if (KEEP_STATES.has(st)) continue;
					const m = meta[id];
					const lu = m && m.lastUpdatedSec ? m.lastUpdatedSec : (timestamps[id] || 0);
					items.push([id, lu]);
				}
				const MAX_NONACTIVE = 100000; // tune as needed
				if (items.length <= MAX_NONACTIVE) return;

				items.sort((a, b) => a[1] - b[1]); // oldest first
				const toDrop = items.length - MAX_NONACTIVE;
				for (let i = 0; i < toDrop; i++) {
					drop(items[i][0]);
				}
			};

			capNonActive(sentMap);
			capNonActive(recvMap);

			// // Write back maps (in case we started with empty objects)
			manager.pollData.sent = sentMap;
			manager.pollData.received = recvMap;
			manager.pollData.timestamps = timestamps;
			manager.pollData.offerMeta = meta;
		})(this);

		this.emit('pollSuccess');

		// If something has changed, emit the event
		if (!deepEqual(origPollData, this.pollData)) {
			this.emit('pollData', this.pollData);
		}

		this._resetPollTimer();
	});
};

TradeOfferManager.prototype._resetPollTimer = function(time) {
	if (this.pollInterval < 0) {
		// timed polling is disabled
		return;
	}

	if (time || this.pollInterval >= this.minimumPollInterval) {
		clearTimeout(this._pollTimer);
		this._pollTimer = setTimeout(this.doPoll.bind(this), time || this.pollInterval);
	}
};
