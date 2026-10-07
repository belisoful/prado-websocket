# Sketch: `prado.pubsub.v1` message format, PHP handler, and browser client

Status: **implemented 2026-10-06 (unreleased, targets v1.2.0).** The shipped code in
`src/IO/Socket/WebSocket/PubSub/` is authoritative. It differs from this sketch in these ways:
- `TWebSocketPubSubException` extends `TWebSocketException`, uses `errorMessages.txt` keys, and
  carries the wire code as `ReplyCode`.
- Handler state is keyed per connection. The handler registers a connection the server didn't.
- `close()` in the JS client takes effect immediately.
- Open-question defaults: `IWebSocketClusterAware` plus module injection; the module re-raises
  the four events; the JS ships in `PubSub/assets/` (`getClientScriptPath()`), not npm; text-only;
  no rate limiting; standalone server only.

## Why

The server speaks plain RFC 6455, so a browser needs no library to connect. But
`TWebSocketCluster::subscribe()/publish()/sendToClient()` are server-side calls only: the
server never reads client messages as commands. Every app that wants "browser joins channel
X" has to invent a wire format, a dispatcher, authorization, and a reconnecting client.
This sketch standardizes those as an **opt-in** layer. A raw `TWebSocketHandler` keeps
working unchanged.

Facts from the source this design relies on:

- Every connection is registered with the cluster **before** the handler's `onOpen`
  (`TWebSocketServer::activateConnection()`, `TWebSocketServer.php:1318`), so
  `$cluster->getClientId($connection)` is valid in `onOpen`. The h2 path registers on the
  protocol's `onConnection` (`:1280`); confirm it also precedes `onOpen`.
- `TWebSocketCluster::publish()` fans **one pre-encoded payload** out to every subscriber,
  so the JSON frame has to be built once by the publisher. It can't be changed per recipient.
- `unregister()` already drops a client's cluster subscriptions on close.
- The handler has **no cluster reference** today. The module creates it from `HandlerClass`
  (`TWebSocketModule::getHandler()`), so the cluster needs to be injected (see "Module change").

---

## 1. Wire format

- **Subprotocol:** `prado.pubsub.v1`, negotiated via `Sec-WebSocket-Protocol`. The version
  is in the name, so a v2 can be offered alongside it.
- **Frames:** one UTF-8 JSON object per Text message. A Binary message closes with
  **1003** (UnsupportedData) in v1.
- **Envelope fields** (all optional except `type`):

| Field | Type | Meaning |
|---|---|---|
| `type` | string | Frame type (tables below) |
| `id` | string (≤ 64 chars) | Client-chosen correlation id, echoed in the reply |
| `channel` | string | `^[A-Za-z0-9_.:/-]{1,128}$` |
| `data` | any JSON | Application payload |
| `to` | string | Target cluster client id (`send`) |
| `from` | string | Sender's cluster client id (server → client; absent = server-originated) |
| `method`, `params` | string, any | Application RPC (`call`) |
| `code`, `message` | string, string | Error code and human text (`error`) |

### Client → server

| `type` | Fields | Effect |
|---|---|---|
| `subscribe` | `channel` | Join a channel (idempotent). Authorized by `onSubscribe`. |
| `unsubscribe` | `channel` | Leave a channel (idempotent). |
| `publish` | `channel`, `data` | Publish to a channel cluster-wide. **Denied by default** (`AllowClientPublish`). |
| `send` | `to`, `data` | Direct message to one client anywhere in the cluster. **Denied by default** (`AllowClientSend`). |
| `call` | `method`, `params` | App RPC through `onCall`. The result returns in the `ack`'s `data`. |
| `ping` | — | Application heartbeat. The server answers `pong`. |

### Server → client

| `type` | Fields | When |
|---|---|---|
| `welcome` | `clientId`, `heartbeat` (seconds) | First frame after open. A new `clientId` comes on every reconnect. |
| `ack` | `id`, `data?` | Success of a request that carried an `id` |
| `error` | `id?`, `code`, `message` | Failure. Without `id` it is a connection-level error. |
| `message` | `data`, `channel?`, `from?` | Delivery. With `channel` it is a channel publish; without, a direct send or broadcast. |
| `pong` | — | Heartbeat reply |

**Reply rule:** a request **with** `id` gets exactly one `ack` or `error`. A request
**without** `id` is fire-and-forget: success is silent and only failures are reported (as
`error` without `id`).

**Error codes:** `bad_request` (malformed frame or field), `unknown_type`, `forbidden`
(authorization veto), `limit` (subscription cap), `not_found` (`call` with no handler, or
`send` to an unknown client), `internal`.

**Close codes:** standard codes for protocol failures (1002 wrong or missing subprotocol,
1003 binary, 1009 too big via `MaxMessageSize`). Applications use **4401**
(unauthenticated) and **4403** (forbidden) to tell the client *not* to reconnect. Any
other code makes the client reconnect with backoff.

**Delivery semantics:** at-most-once, with no history or replay. A message published while
a client is reconnecting is lost to that client. Apps that need gap-free delivery should
`call` a history method after `welcome`.

### Example exchange

```text
→ (handshake, Sec-WebSocket-Protocol: prado.pubsub.v1)
← {"type":"welcome","clientId":"edge-1-1759740000-17","heartbeat":25}
→ {"type":"subscribe","id":"1","channel":"room:42"}
← {"type":"ack","id":"1"}
→ {"type":"publish","id":"2","channel":"room:42","data":{"text":"hi"}}
← {"type":"error","id":"2","code":"forbidden","message":"publish to room:42 denied"}
← {"type":"message","channel":"room:42","data":{"text":"hello from PHP"}}
→ {"type":"call","id":"3","method":"chat.history","params":{"room":42}}
← {"type":"ack","id":"3","data":[{"text":"…"}]}
```

---

## 2. PHP: `Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubHandler`

Extends `TWebSocketHandler`, so `handleConnection()` and the lifecycle events stay intact.
It overrides `onOpen`/`onMessage`/`onClose`, and raises new events (`onSubscribe`,
`onPublish`, `onSend`, `onCall`) with a `TWebSocketPubSubEventParameter` whose `Allowed`
an app handler may flip.

```php
namespace Prado\IO\Socket\WebSocket\PubSub;

use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;

/**
 * @since 1.2.0
 */
class TWebSocketPubSubHandler extends TWebSocketHandler implements IWebSocketClusterAware
{
	public const SUBPROTOCOL = 'prado.pubsub.v1';
	public const CHANNEL_PATTERN = '/^[A-Za-z0-9_.:\/-]{1,128}$/';
	private const JSON_FLAGS = JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE;

	private ?TWebSocketCluster $_cluster = null;
	private bool $_allowClientPublish = false;
	private bool $_allowClientSend = false;
	private int $_maxSubscriptions = 64;
	private float $_heartbeat = 25.0;   // keep below the server IdleTimeout
	/** @var array<string, array<string, true>> local client id → its channels */
	private array $_subscriptions = [];

	public function onOpen(TWebSocketConnection $connection): void
	{
		if ($connection->getSubprotocol() !== self::SUBPROTOCOL) {
			$connection->close(TWebSocketCloseCode::ProtocolError, 'subprotocol ' . self::SUBPROTOCOL . ' required');
			return;
		}
		$clientId = $this->getCluster()->getClientId($connection);
		$this->_subscriptions[$clientId] = [];
		$this->reply($connection, ['type' => 'welcome', 'clientId' => $clientId, 'heartbeat' => $this->_heartbeat]);
		parent::onOpen($connection);
	}

	public function onMessage(TWebSocketConnection $connection, string $message, int $opcode): void
	{
		if ($opcode !== TWebSocketOpcode::Text) {
			$connection->close(TWebSocketCloseCode::UnsupportedData, 'prado.pubsub.v1 is text-only');
			return;
		}
		$clientId = $this->getCluster()->getClientId($connection);
		$frame = json_decode($message, true, 16);
		$id = $frame['id'] ?? null;
		try {
			if (!is_array($frame) || !is_string($frame['type'] ?? null) || ($id !== null && (!is_string($id) || strlen($id) > 64))) {
				$id = null;
				throw new TWebSocketPubSubError('bad_request', 'malformed frame');
			}
			if ($frame['type'] === 'ping') {
				$this->reply($connection, ['type' => 'pong']);
				return;
			}
			$data = match ($frame['type']) {
				'subscribe' => $this->handleSubscribe($connection, $clientId, $frame),
				'unsubscribe' => $this->handleUnsubscribe($clientId, $frame),
				'publish' => $this->handlePublish($connection, $clientId, $frame),
				'send' => $this->handleSend($connection, $clientId, $frame),
				'call' => $this->handleCall($connection, $clientId, $frame),
				default => throw new TWebSocketPubSubError('unknown_type', "unknown type {$frame['type']}"),
			};
			if ($id !== null) {
				$this->reply($connection, $data === null ? ['type' => 'ack', 'id' => $id] : ['type' => 'ack', 'id' => $id, 'data' => $data]);
			}
		} catch (TWebSocketPubSubError $e) {
			$this->reply($connection, array_filter(['type' => 'error', 'id' => $id, 'code' => $e->getErrorCode(), 'message' => $e->getMessage()], fn ($v) => $v !== null));
		}
		// The base onMessage event is not raised: pub/sub frames are protocol, not app messages.
		// App messages travel as `call`.
	}

	public function onClose(TWebSocketConnection $connection): void
	{
		if (($clientId = $this->getCluster()->getClientId($connection)) !== null) {
			unset($this->_subscriptions[$clientId]);   // the cluster drops its own subscriptions on unregister
		}
		parent::onClose($connection);
	}

	protected function handleSubscribe(TWebSocketConnection $connection, string $clientId, array $frame): mixed
	{
		$channel = $this->ensureChannel($frame);
		if (isset($this->_subscriptions[$clientId][$channel])) {
			return null;
		}
		if (count($this->_subscriptions[$clientId] ?? []) >= $this->_maxSubscriptions) {
			throw new TWebSocketPubSubError('limit', "at most {$this->_maxSubscriptions} subscriptions");
		}
		$this->authorize('onSubscribe', $connection, new TWebSocketPubSubEventParameter($clientId, $channel, null, true));
		$this->_subscriptions[$clientId][$channel] = true;
		$this->getCluster()->subscribe($clientId, $channel);
		return null;
	}

	protected function handleUnsubscribe(string $clientId, array $frame): mixed
	{
		$channel = $this->ensureChannel($frame);
		unset($this->_subscriptions[$clientId][$channel]);
		$this->getCluster()->unsubscribe($clientId, $channel);
		return null;
	}

	protected function handlePublish(TWebSocketConnection $connection, string $clientId, array $frame): mixed
	{
		$channel = $this->ensureChannel($frame);
		$param = $this->authorize('onPublish', $connection, new TWebSocketPubSubEventParameter($clientId, $channel, $frame['data'] ?? null, $this->_allowClientPublish));
		$this->publish($channel, $param->getData(), $clientId);   // a handler may rewrite or sanitize Data
		return null;
	}

	protected function handleSend(TWebSocketConnection $connection, string $clientId, array $frame): mixed
	{
		$to = $frame['to'] ?? null;
		if (!is_string($to) || $to === '') {
			throw new TWebSocketPubSubError('bad_request', 'send requires "to"');
		}
		$param = $this->authorize('onSend', $connection, new TWebSocketPubSubEventParameter($clientId, null, $frame['data'] ?? null, $this->_allowClientSend, $to));
		if (!$this->getCluster()->sendToClient($to, self::encodeMessage($param->getData(), null, $clientId))) {
			throw new TWebSocketPubSubError('not_found', 'unknown client');
		}
		return null;
	}

	protected function handleCall(TWebSocketConnection $connection, string $clientId, array $frame): mixed
	{
		if (!is_string($frame['method'] ?? null)) {
			throw new TWebSocketPubSubError('bad_request', 'call requires "method"');
		}
		$param = new TWebSocketPubSubEventParameter($clientId, null, $frame['params'] ?? null, false, null, $frame['method']);
		$this->raiseEvent('onCall', $connection, $param);   // a handler sets Result (and Handled), or throws TWebSocketPubSubError
		if (!$param->getHandled()) {
			throw new TWebSocketPubSubError('not_found', "no method {$frame['method']}");
		}
		return $param->getResult();
	}

	/** Raises an authorization event and throws `forbidden` on a veto. */
	protected function authorize(string $event, TWebSocketConnection $connection, TWebSocketPubSubEventParameter $param): TWebSocketPubSubEventParameter
	{
		$this->raiseEvent($event, $connection, $param);
		if (!$param->getAllowed()) {
			throw new TWebSocketPubSubError('forbidden', substr($event, 2) . ' denied');
		}
		return $param;
	}

	protected function ensureChannel(array $frame): string
	{
		$channel = $frame['channel'] ?? null;
		if (!is_string($channel) || !preg_match(self::CHANNEL_PATTERN, $channel)) {
			throw new TWebSocketPubSubError('bad_request', 'invalid channel');
		}
		return $channel;
	}

	protected function reply(TWebSocketConnection $connection, array $frame): void
	{
		$connection->send(json_encode($frame, self::JSON_FLAGS));
	}

	// ---- Server-side API for application code -------------------------------

	public function publish(string $channel, mixed $data, ?string $from = null): void
	{
		$this->getCluster()->publish($channel, self::encodeMessage($data, $channel, $from));
	}

	public function sendTo(string $clientId, mixed $data): bool
	{
		return $this->getCluster()->sendToClient($clientId, self::encodeMessage($data));
	}

	public function broadcast(mixed $data): void
	{
		$this->getCluster()->broadcast(self::encodeMessage($data));
	}

	/** Builds a `message` frame. Static so code with only the module can call `$module->publish($ch, TWebSocketPubSubHandler::encodeMessage($d, $ch))`. */
	public static function encodeMessage(mixed $data, ?string $channel = null, ?string $from = null): string
	{
		$frame = ['type' => 'message'];
		if ($channel !== null) {
			$frame['channel'] = $channel;
		}
		if ($from !== null) {
			$frame['from'] = $from;
		}
		$frame['data'] = $data;
		return json_encode($frame, self::JSON_FLAGS);
	}

	// getCluster()/setCluster() (IWebSocketClusterAware), plus the Allow* /
	// MaxSubscriptions / Heartbeat properties with TPropertyValue coercion, and the
	// onSubscribe/onPublish/onSend/onCall event methods, all elided.
}
```

Supporting types (each small):

- `TWebSocketPubSubEventParameter extends \Prado\TEventParameter`: `ClientId`, `Channel`,
  `Data` (read/write), `Allowed` (read/write), `To`, `Method`, `Result` (write sets
  `Handled`).
- `TWebSocketPubSubError extends \RuntimeException`: carries the string `ErrorCode`. It is
  a reply error, not a connection close. It deliberately isn't a `TException`, because its
  message is wire text rather than an `errorMessages.txt` key.
- `IWebSocketClusterAware`: `setCluster(TWebSocketCluster)` / `getCluster()`.

### Module change (compatible)

In `TWebSocketModule::setHandler()`, inject the cluster into a cluster-aware handler:

```php
if ($value instanceof IWebSocketClusterAware) {
	$value->setCluster($this->getCluster());
}
```

### App usage

```xml
<module id="websockets" class="Prado\IO\Socket\WebSocket\TWebSocketModule"
	Port="8080" Subprotocols="prado.pubsub.v1" IdleTimeout="60"
	HandlerClass="Application\Chat\ChatHandler" />
```

```php
class ChatHandler extends TWebSocketPubSubHandler
{
	public function __construct()
	{
		parent::__construct();
		$this->attachEventHandler('onSubscribe', function ($connection, $param) {
			$param->setAllowed(str_starts_with($param->getChannel(), 'room:'));
		});
		$this->attachEventHandler('onPublish', function ($connection, $param) {
			$param->setAllowed(str_starts_with($param->getChannel(), 'room:'));
			$param->setData(['text' => mb_substr((string) ($param->getData()['text'] ?? ''), 0, 2000)]);
		});
		$this->attachEventHandler('onCall', function ($connection, $param) {
			if ($param->getMethod() === 'chat.history') {
				$param->setResult(ChatStore::recent((int) ($param->getData()['room'] ?? 0)));
			}
		});
	}
}
```

**Authentication** is outside the pub/sub layer. It happens at the handshake (same-origin
cookie, or a short-lived token in the URL query) and is checked in `onOpen`. A failure is
closed with **4401**, so the client stops reconnecting.

---

## 3. Browser client: `prado-pubsub.js`

A dependency-free ES2017 file with a UMD wrapper (global `Prado.WebSocket.PubSub`, or
CommonJS). Features:

- negotiates the subprotocol
- promise-based requests with timeouts
- automatic reconnect with exponential backoff and full jitter
- re-subscribes after reconnect
- queues frames while offline (bounded)
- an app heartbeat to detect half-open sockets, which browsers can't otherwise detect
- reconnects immediately on the `online` event
- does not reconnect on fatal close codes

```js
/*! prado-websocket pub/sub client (prado.pubsub.v1) */
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
		constructor(code, message) {
			super(message || code);
			this.name = 'PubSubError';
			this.code = code;
		}
	}

	class PubSub {
		/**
		 * @param {string} url  ws:// or wss:// endpoint
		 * @param {object} [options]
		 */
		constructor(url, options = {}) {
			this.url = url;
			this.options = Object.assign({
				reconnect: true,
				minDelay: 500,          // ms, first backoff ceiling
				maxDelay: 30000,        // ms, backoff cap
				requestTimeout: 10000,  // ms, per request once sent
				maxQueue: 256,          // frames held while offline
				fatalCloseCodes: [1002, 1003, 1007, 1008, 1009, 1010, 4401, 4403],
			}, options);
			this.clientId = null;
			this.state = 'connecting';  // connecting | open | reconnecting | closed
			this._ws = null;
			this._attempt = 0;
			this._nextId = 1;
			this._pending = new Map();  // id -> {frame, resolve, reject, retry, sent, timer}
			this._queue = [];           // frames awaiting an open socket
			this._channels = new Map(); // channel -> {handlers: Set, acked: bool, ready: Promise}
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

		/** Subscribes a handler(data, frame). Resolves to an unsubscribe function once the server acks. */
		subscribe(channel, handler) {
			let entry = this._channels.get(channel);
			if (!entry) {
				entry = { handlers: new Set(), acked: false, ready: null };
				this._channels.set(channel, entry);
				entry.ready = this._request({ type: 'subscribe', channel }, true).then(() => { entry.acked = true; });
				entry.ready.catch(() => {
					if (this._channels.get(channel) === entry) {
						this._channels.delete(channel);
					}
				});
			}
			entry.handlers.add(handler);
			return entry.ready.then(() => () => this._off(channel, handler));
		}

		publish(channel, data) { return this._request({ type: 'publish', channel, data }); }
		send(to, data) { return this._request({ type: 'send', to, data }); }
		call(method, params) { return this._request({ type: 'call', method, params }); }

		/** Events: open({clientId}), close({code, reason, willReconnect}), message(data, frame), error(err). */
		on(event, fn) {
			if (!this._listeners.has(event)) {
				this._listeners.set(event, new Set());
			}
			this._listeners.get(event).add(fn);
			return () => this._listeners.get(event).delete(fn);
		}

		close() {
			this._closedByUser = true;
			clearTimeout(this._reconnectTimer);
			if (typeof removeEventListener === 'function') {
				removeEventListener('online', this._onOnline);
			}
			if (this._ws) {
				this._ws.close(1000);   // onclose finalizes
			} else {
				this._finalize(new PubSubError('closed', 'client closed'));
			}
		}

		// ---- Connection -----------------------------------------------------

		_connect() {
			this.state = 'connecting';
			const ws = new WebSocket(this.url, SUBPROTOCOL);
			this._ws = ws;
			ws.onopen = () => {
				if (ws.protocol !== SUBPROTOCOL) {
					this._fatal = new PubSubError('subprotocol', 'server did not negotiate ' + SUBPROTOCOL);
					ws.close(1000);
					return;
				}
				this.state = 'open';
				this._lastSeen = Date.now();
				for (const [channel, entry] of this._channels) {
					if (entry.acked) {
						this._request({ type: 'subscribe', channel }, true)
							.catch((e) => { this._channels.delete(channel); this._emit('error', e); });
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
			ws.onerror = () => {};   // onclose always follows
		}

		_closed(ws, code, reason) {
			if (ws !== this._ws) {
				return;   // a socket already dropped by the heartbeat
			}
			this._ws = null;
			this.clientId = null;
			clearInterval(this._heartbeatTimer);
			const willReconnect = this.options.reconnect && !this._closedByUser && !this._fatal
				&& !this.options.fatalCloseCodes.includes(code);
			// In-flight requests: idempotent ones (subscribe) are re-queued first; the rest fail,
			// since the server may or may not have applied them.
			const requeue = [];
			for (const [id, p] of this._pending) {
				if (!p.sent) {
					continue;   // still in the queue
				}
				clearTimeout(p.timer);
				p.sent = false;
				if (p.retry && willReconnect) {
					requeue.push(p.frame);
				} else {
					this._pending.delete(id);
					p.reject(new PubSubError('disconnected', 'connection lost before reply'));
				}
			}
			this._queue.unshift(...requeue);
			this._emit('close', { code, reason, willReconnect });
			if (!willReconnect) {
				this._finalize(this._fatal || new PubSubError('closed', 'connection closed (' + code + ')'));
				return;
			}
			this.state = 'reconnecting';
			const ceiling = Math.min(this.options.maxDelay, this.options.minDelay * 2 ** this._attempt++);
			this._reconnectTimer = setTimeout(() => this._connect(), Math.random() * ceiling);
		}

		_drop(code, reason) {
			const ws = this._ws;
			ws.onopen = ws.onmessage = ws.onclose = null;
			try { ws.close(code, reason); } catch (e) { /* already closing */ }
			this._closed(ws, code, reason);
		}

		_finalize(error) {
			this.state = 'closed';
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
					this._drop(4000, 'heartbeat timeout');   // half-open socket: don't wait for the close handshake
				} else {
					this._transmit({ type: 'ping' });
				}
			}, ms);
		}

		// ---- Frames ---------------------------------------------------------

		_request(frame, retry = false) {
			return new Promise((resolve, reject) => {
				frame.id = String(this._nextId++);
				const p = { frame, resolve, reject, retry, sent: false, timer: null };
				this._pending.set(frame.id, p);
				if (this.state === 'closed') {
					this._pending.delete(frame.id);
					reject(new PubSubError('closed', 'client is closed'));
				} else if (this.state === 'open') {
					this._transmit(frame);
				} else if (this._queue.length >= this.options.maxQueue) {
					this._pending.delete(frame.id);
					reject(new PubSubError('queue_full', 'offline queue is full'));
				} else {
					this._queue.push(frame);
				}
			});
		}

		_transmit(frame) {
			this._ws.send(JSON.stringify(frame));
			const p = frame.id && this._pending.get(frame.id);
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
			if (!entry || !entry.handlers.delete(handler) || entry.handlers.size) {
				return;
			}
			this._channels.delete(channel);
			if (this.state === 'open') {
				this._transmit({ type: 'unsubscribe', channel });
			} else if (this.state !== 'closed') {
				this._queue.push({ type: 'unsubscribe', channel });   // cancels a subscribe still queued
			}
		}

		_receive(raw) {
			this._lastSeen = Date.now();
			let msg;
			try {
				msg = JSON.parse(raw);
			} catch (e) {
				this._emit('error', new PubSubError('bad_frame', 'unparseable frame'));
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
					const p = msg.id != null && this._pending.get(msg.id);
					if (p) {
						clearTimeout(p.timer);
						this._pending.delete(msg.id);
						msg.type === 'ack' ? p.resolve(msg.data) : p.reject(new PubSubError(msg.code, msg.message));
					} else if (msg.type === 'error') {
						this._emit('error', new PubSubError(msg.code, msg.message));
					}
					break;
				}
				case 'message':
					if (msg.channel != null) {
						const entry = this._channels.get(msg.channel);
						entry && entry.handlers.forEach((h) => this._safely(h, msg.data, msg));
					} else {
						this._emit('message', msg.data, msg);
					}
					break;
				case 'pong':
					break;
			}
		}

		_emit(event, ...args) {
			const set = this._listeners.get(event);
			set && set.forEach((fn) => this._safely(fn, ...args));
		}

		_safely(fn, ...args) {
			try { fn(...args); } catch (e) { setTimeout(() => { throw e; }); }   // a handler bug must not break dispatch
		}
	}

	PubSub.SUBPROTOCOL = SUBPROTOCOL;
	PubSub.PubSubError = PubSubError;
	return PubSub;
}));
```

### Browser usage

```html
<script src="/assets/prado-pubsub.js"></script>
<script>
	const ps = new Prado.WebSocket.PubSub('wss://example.com/ws');
	ps.on('open', ({ clientId }) => console.log('connected as', clientId));
	ps.on('close', ({ code, willReconnect }) => console.log('closed', code, willReconnect));

	const leave = await ps.subscribe('room:42', (data) => render(data));
	await ps.publish('room:42', { text: 'hi' });          // rejects with code "forbidden" if denied
	const history = await ps.call('chat.history', { room: 42 });
	leave();                                               // unsubscribe
</script>
```

---

## 4. Tests

- **Unit** (`tests/unit/IO/Socket/WebSocket/PubSub/`): drive the handler with an in-memory
  `TWebSocketConnection` and a `TWebSocketCluster` on `TNullBackplane`. Cover:
  - the welcome frame and a missing subprotocol (1002); a binary frame (1003)
  - malformed JSON, a bad `id`, an unknown type
  - an invalid channel and the subscription cap
  - default-deny for publish and send, and a vetoing event handler
  - `Data` rewrite on publish; `call` both handled and unhandled
  - the reply rule (no reply on success without `id`)
  - `encodeMessage()` shape; subscriptions cleared on close
- **Playwright** (`tests/playwright/pubsub.spec.js`): run `ws-server.php` with the pub/sub
  handler and load `prado-pubsub.js` with `page.addScriptTag`. Cover:
  - a subscribe/publish round trip between two pages
  - a server restart, after which the client reconnects, gets a new `clientId`, and
    re-subscribes
  - close 4401 does not reconnect
  - an offline queue flushes on reconnect
  - a heartbeat timeout triggers a reconnect

## 5. Open questions

1. **Names:** `prado.pubsub.v1`, the `PubSub` sub-namespace, and the file name
   `prado-pubsub.js`.
2. **Cluster injection:** use `IWebSocketClusterAware` plus the `setHandler()` hook (above),
   or have the handler resolve the module through the application.
3. **Module event forwarding:** should the module re-raise `onSubscribe`/`onPublish`/
   `onSend`/`onCall` like it does the lifecycle events? That would allow
   `OnPublish="Application.Chat.onPublish"` in XML.
4. **JS distribution:** publish via PRADO's asset manager (a small `TWebSocketClientScript`
   helper that registers the file), ship it through npm with `exports` in `package.json`,
   or both. If via npm, also ship an ESM build.
5. **Binary payloads:** keep v1 text-only, or add a binary sub-format later.
6. **Rate limiting:** there is none per client today beyond `MaxMessageSize`. Add a simple
   token bucket in the handler (frames/sec), or leave it to the app?
7. **SAPI path:** `TWebSocketService` has no cluster. Pub/sub there would be local-only or
   unsupported. Document it as standalone-server-only for v1.
