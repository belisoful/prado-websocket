/*!
 * prado-pubsub.js: the browser client of the prado.pubsub.v1 WebSocket subprotocol.
 * Served by Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubHandler.
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 * @since 1.2.0
 */

/*
 * Usage:
 *
 *   const ps = new Prado.WebSocket.PubSub('wss://example.com/ws');
 *   ps.on('open', ({ clientId }) => {});
 *   const leave = await ps.subscribe('room:42', (data, frame) => {});
 *   await ps.publish('room:42', { text: 'hi' });
 *   const rows = await ps.call('chat.history', { room: 42 });
 *   leave();
 *   ps.close();
 *
 * Events: open({clientId}), close({code, reason, willReconnect}), message(data, frame), error(PubSubError).
 * Requests reject with a PubSubError whose `code` is the server's reply code, or one of the
 * client codes: timeout, disconnected, closed, queue_full.
 *
 * The client reconnects with exponential backoff and full jitter, re-subscribes its channels,
 * holds requests made while offline (up to maxQueue), and pings at the server's heartbeat
 * interval, dropping a connection silent for twice that.  It stops reconnecting after close()
 * or a close code in fatalCloseCodes.  A refused handshake (a rejected origin) is reported by the
 * browser as 1006 and retried with backoff.  A server that does not select prado.pubsub.v1 is
 * refused for good with the `subprotocol` code where the browser opens the socket (Firefox,
 * WebKit); Chromium fails that handshake itself, as 1006.  A publish, send or call in flight
 * when the connection drops rejects with `disconnected` and is not resent.
 */
(function (root, factory) {
	if (typeof module === 'object' && module.exports) {
		module.exports = factory();
	} else {
		root.Prado = root.Prado || {};
		root.Prado.WebSocket = root.Prado.WebSocket || {};
		root.Prado.WebSocket.PubSub = factory();
	}
}(typeof self !== 'undefined' ? self : this, function () {
	'use strict';

	const SUBPROTOCOL = 'prado.pubsub.v1';

	class PubSubError extends Error {
		/**
		 * @param {string} code The reply code.
		 * @param {string} [message] The human-readable message.
		 */
		constructor(code, message) {
			super(message || code);
			this.name = 'PubSubError';
			this.code = code;
		}
	}

	class PubSub {
		/**
		 * Opens the connection.
		 * @param {string} url The ws:// or wss:// endpoint.
		 * @param {object} [options]
		 * @param {boolean} [options.reconnect=true] Whether to reconnect after an unexpected close.
		 * @param {number} [options.minDelay=500] The first backoff ceiling in ms.
		 * @param {number} [options.maxDelay=30000] The backoff cap in ms.
		 * @param {number} [options.requestTimeout=10000] The ms a sent request waits for its reply.
		 * @param {number} [options.maxQueue=256] The frames held while offline.
		 * @param {number[]} [options.fatalCloseCodes] The close codes that end reconnection.
		 * @param {Function} [options.WebSocket] The WebSocket constructor, for non-browser hosts.
		 */
		constructor(url, options = {}) {
			this.url = url;
			this.options = Object.assign({
				reconnect: true,
				minDelay: 500,
				maxDelay: 30000,
				requestTimeout: 10000,
				maxQueue: 256,
				fatalCloseCodes: [1002, 1003, 1007, 1008, 1009, 1010, 4401, 4403],
				WebSocket: typeof WebSocket === 'function' ? WebSocket : null,
			}, options);
			/** @type {?string} The cluster client id from the latest welcome. */
			this.clientId = null;
			/** @type {'connecting'|'open'|'reconnecting'|'closed'} */
			this.state = 'connecting';
			this._ws = null;
			this._attempt = 0;
			this._nextId = 1;
			this._pending = new Map();
			this._queue = [];
			this._channels = new Map();
			this._listeners = new Map();
			this._reconnectTimer = null;
			this._heartbeatTimer = null;
			this._lastSeen = 0;
			this._closedByUser = false;
			this._fatal = null;
			this._onOnline = () => {
				if (this.state === 'reconnecting') {
					clearTimeout(this._reconnectTimer);
					this._connect();
				}
			};
			if (typeof addEventListener === 'function') {
				addEventListener('online', this._onOnline);
			}
			this._connect();
		}

		// ---- Public API -----------------------------------------------------

		/**
		 * Subscribes a handler to a channel.  Handlers of one channel share one server subscription.
		 * @param {string} channel The channel name.
		 * @param {(data: any, frame: object) => void} handler Called with each message.
		 * @returns {Promise<() => void>} Resolves to an unsubscribe function once the server acks.
		 */
		subscribe(channel, handler) {
			let entry = this._channels.get(channel);
			if (!entry) {
				entry = { handlers: new Set(), acked: false, ready: null };
				this._channels.set(channel, entry);
				entry.ready = this._request({ type: 'subscribe', channel }, true).then(() => {
					entry.acked = true;
				});
				entry.ready.catch(() => {
					if (this._channels.get(channel) === entry) {
						this._channels.delete(channel);
					}
				});
			}
			entry.handlers.add(handler);
			return entry.ready.then(() => () => this._off(channel, handler));
		}

		/**
		 * Publishes data to a channel.
		 * @param {string} channel The channel name.
		 * @param {any} data The JSON-encodable data.
		 * @returns {Promise<void>}
		 */
		publish(channel, data) {
			return this._request({ type: 'publish', channel, data });
		}

		/**
		 * Sends data to one client.
		 * @param {string} to The target client id.
		 * @param {any} data The JSON-encodable data.
		 * @returns {Promise<void>}
		 */
		send(to, data) {
			return this._request({ type: 'send', to, data });
		}

		/**
		 * Calls an application method on the server.
		 * @param {string} method The method name.
		 * @param {any} [params] The JSON-encodable parameters.
		 * @returns {Promise<any>} The result.
		 */
		call(method, params) {
			return this._request({ type: 'call', method, params });
		}

		/**
		 * Adds an event listener.
		 * @param {'open'|'close'|'message'|'error'} event The event name.
		 * @param {Function} fn The listener.
		 * @returns {() => void} A function removing the listener.
		 */
		on(event, fn) {
			if (!this._listeners.has(event)) {
				this._listeners.set(event, new Set());
			}
			this._listeners.get(event).add(fn);
			return () => this._listeners.get(event).delete(fn);
		}

		/**
		 * Closes the connection for good.  The client is closed on return: queued requests reject
		 * with `closed`, requests in flight with `disconnected`, and later requests with `closed`.
		 */
		close() {
			if (this.state === 'closed') {
				return;
			}
			this._closedByUser = true;
			clearTimeout(this._reconnectTimer);
			if (this._ws) {
				this._drop(1000, 'client closed');
			} else {
				this._finalize(new PubSubError('closed', 'client closed'));
			}
		}

		// ---- Connection -----------------------------------------------------

		_connect() {
			this.state = 'connecting';
			const ws = new this.options.WebSocket(this.url, SUBPROTOCOL);
			this._ws = ws;
			ws.onopen = () => {
				if (ws.protocol !== SUBPROTOCOL) {
					this._fatal = new PubSubError('subprotocol', 'the server did not negotiate ' + SUBPROTOCOL);
					ws.close(1000);
					return;
				}
				this.state = 'open';
				this._lastSeen = Date.now();
				for (const [channel, entry] of this._channels) {
					if (entry.acked) {
						this._request({ type: 'subscribe', channel }, true).catch((e) => {
							this._channels.delete(channel);
							this._emit('error', e);
						});
					}
				}
				const queued = this._queue;
				this._queue = [];
				queued.forEach((frame) => this._transmit(frame));
			};
			ws.onmessage = (ev) => {
				if (typeof ev.data === 'string') {
					this._receive(ev.data);
				}
			};
			ws.onclose = (ev) => this._closed(ws, ev.code, ev.reason);
			ws.onerror = () => {};
		}

		_closed(ws, code, reason) {
			if (ws !== this._ws) {
				return;
			}
			this._ws = null;
			this.clientId = null;
			clearInterval(this._heartbeatTimer);
			const willReconnect = this.options.reconnect && !this._closedByUser && !this._fatal
				&& !this.options.fatalCloseCodes.includes(code);
			// A sent subscribe is requeued (it is idempotent); any other sent request may or may not
			// have been applied, so it fails rather than risk a duplicate.
			const requeue = [];
			for (const [id, p] of this._pending) {
				if (!p.sent) {
					continue;
				}
				clearTimeout(p.timer);
				p.sent = false;
				if (p.retry && willReconnect) {
					requeue.push(p.frame);
				} else {
					this._pending.delete(id);
					p.reject(new PubSubError('disconnected', 'the connection closed before the reply'));
				}
			}
			this._queue.unshift(...requeue);
			this.state = willReconnect ? 'reconnecting' : 'closed';
			this._emit('close', { code, reason, willReconnect });
			if (!willReconnect) {
				this._finalize(this._fatal || new PubSubError('closed', 'the connection closed (' + code + ')'));
				return;
			}
			const ceiling = Math.min(this.options.maxDelay, this.options.minDelay * 2 ** this._attempt++);
			this._reconnectTimer = setTimeout(() => this._connect(), Math.random() * ceiling);
		}

		_drop(code, reason) {
			const ws = this._ws;
			ws.onopen = ws.onmessage = ws.onclose = null;
			try {
				ws.close(code, reason);
			} catch (e) {
				// already closing
			}
			this._closed(ws, code, reason);
		}

		_finalize(error) {
			this.state = 'closed';
			if (typeof removeEventListener === 'function') {
				removeEventListener('online', this._onOnline);
			}
			for (const p of this._pending.values()) {
				clearTimeout(p.timer);
				p.reject(error);
			}
			this._pending.clear();
			this._queue = [];
		}

		_startHeartbeat(seconds) {
			clearInterval(this._heartbeatTimer);
			if (!(seconds > 0)) {
				return;
			}
			const ms = seconds * 1000;
			this._heartbeatTimer = setInterval(() => {
				if (Date.now() - this._lastSeen > 2 * ms) {
					this._drop(4000, 'heartbeat timeout');
				} else {
					this._transmit({ type: 'ping' });
				}
			}, ms);
		}

		// ---- Frames ---------------------------------------------------------

		_request(frame, retry = false) {
			return new Promise((resolve, reject) => {
				if (this.state === 'closed') {
					reject(new PubSubError('closed', 'the client is closed'));
					return;
				}
				if (this.state !== 'open' && this._queue.length >= this.options.maxQueue) {
					reject(new PubSubError('queue_full', 'the offline queue is full'));
					return;
				}
				frame.id = String(this._nextId++);
				this._pending.set(frame.id, { frame, resolve, reject, retry, sent: false, timer: null });
				if (this.state === 'open') {
					this._transmit(frame);
				} else {
					this._queue.push(frame);
				}
			});
		}

		_transmit(frame) {
			this._ws.send(JSON.stringify(frame));
			const p = frame.id !== undefined ? this._pending.get(frame.id) : null;
			if (p) {
				p.sent = true;
				p.timer = setTimeout(() => {
					this._pending.delete(frame.id);
					p.reject(new PubSubError('timeout', 'no reply within ' + this.options.requestTimeout + ' ms'));
				}, this.options.requestTimeout);
			}
		}

		_off(channel, handler) {
			const entry = this._channels.get(channel);
			if (!entry || !entry.handlers.delete(handler) || entry.handlers.size > 0) {
				return;
			}
			this._channels.delete(channel);
			if (this.state === 'open') {
				this._transmit({ type: 'unsubscribe', channel });
			} else if (this.state !== 'closed') {
				this._queue.push({ type: 'unsubscribe', channel });
			}
		}

		_receive(raw) {
			this._lastSeen = Date.now();
			let msg;
			try {
				msg = JSON.parse(raw);
			} catch (e) {
				this._emit('error', new PubSubError('bad_frame', 'the server sent an unparseable frame'));
				return;
			}
			switch (msg.type) {
				case 'welcome':
					this.clientId = msg.clientId;
					this._attempt = 0;
					this._startHeartbeat(msg.heartbeat);
					this._emit('open', { clientId: msg.clientId });
					break;
				case 'ack':
				case 'error': {
					const p = msg.id !== undefined ? this._pending.get(String(msg.id)) : null;
					if (p) {
						clearTimeout(p.timer);
						this._pending.delete(String(msg.id));
						if (msg.type === 'ack') {
							p.resolve(msg.data);
						} else {
							p.reject(new PubSubError(msg.code, msg.message));
						}
					} else if (msg.type === 'error') {
						this._emit('error', new PubSubError(msg.code, msg.message));
					}
					break;
				}
				case 'message':
					if (msg.channel !== undefined) {
						const entry = this._channels.get(msg.channel);
						if (entry) {
							entry.handlers.forEach((h) => this._safely(h, msg.data, msg));
						}
					} else {
						this._emit('message', msg.data, msg);
					}
					break;
				default:
					break;
			}
		}

		_emit(event, ...args) {
			const set = this._listeners.get(event);
			if (set) {
				set.forEach((fn) => this._safely(fn, ...args));
			}
		}

		_safely(fn, ...args) {
			try {
				fn(...args);
			} catch (e) {
				// A listener error is reported asynchronously, so dispatch to the others continues.
				setTimeout(() => {
					throw e;
				});
			}
		}
	}

	PubSub.SUBPROTOCOL = SUBPROTOCOL;
	PubSub.PubSubError = PubSubError;
	return PubSub;
}));
