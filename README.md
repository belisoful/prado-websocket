# PRADO WebSockets Extension

WebSockets for the [PRADO PHP Framework](https://github.com/pradosoft/prado) (version 4.4+), implemented as a PRADO 4 extension:

- **[RFC 6455](https://www.rfc-editor.org/rfc/rfc6455.html) over HTTP/1.1** — the classic `Upgrade` handshake, one WebSocket per connection. The base capability; needs only PHP and PRADO.
- **[RFC 8441](https://www.rfc-editor.org/rfc/rfc8441.html) over HTTP/2** — Extended CONNECT, many WebSockets multiplexed over one connection. **Optional**: enabled only when the [`prado-http2`](https://github.com/belisoful/prado-http2) extension and the system `libnghttp2` are present.
- **[RFC 7692](https://www.rfc-editor.org/rfc/rfc7692.html) permessage-deflate** — negotiated per-message compression, layered on either transport. **Optional**: enabled by offering the extension; needs `ext-zlib`.

A **clustering** layer (`TWebSocketModule` + pluggable backplanes) additionally lets many server processes act as one logical endpoint, so a publish or presence change on any node reaches clients on every node.

The standalone `TWebSocketServer` owns its listening socket end to end, so it completes the upgrade and streams frames in its own process — and **auto-selects HTTP/1.1 or HTTP/2 per connection** from its first bytes (it serves HTTP/1.1 only when HTTP/2 is unavailable). Accepted connections stay non-blocking through the handshake, so a silent or slow client never stalls the loop. A typical web SAPI (PHP-FPM, mod_php) cannot do WebSockets: the web server owns the socket and FastCGI cannot hand it to PHP. Run this as a long-lived server process instead.

## Requirements

| Requirement | Scope | Purpose |
|---|---|---|
| PHP 8.1 – 8.5 | required | The runtime; HTTP/1.1 WebSockets need only this and PRADO. CI runs every minor from 8.1 through 8.5 |
| PRADO Framework `^4.4` | required | `TSocketServer`, `TSocketStream`, the `TStream` IO layer, `TComponent`/`TService`/`TModule` |
| `belisoful/prado-http2` `^1.1` | suggested | The HTTP/2 (RFC 8441) stack; without it the server serves HTTP/1.1 only |
| `ext-ffi` | suggested | Required by `prado-http2` to bind `libnghttp2` |
| System `libnghttp2` | suggested | The HTTP/2 framing engine, loaded at runtime by `prado-http2` |
| `ext-openssl` | suggested | TLS with ALPN — `wss://`, and `h2` for HTTP/2 over TLS |
| `ext-sockets` | suggested | Faster socket primitives for the standalone server |
| `ext-zlib` | suggested | RFC 7692 permessage-deflate message compression |
| `ext-redis` | suggested | The Redis-backed cluster backplane (`TRedisBackplane`) for multi-host scaling |

HTTP/2 is **opt-in**. Add it with:

```sh
composer require belisoful/prado-http2        # then: brew install libnghttp2  (or apt-get install libnghttp2-dev)
```

`TWebSocketServer::isHttp2Available()` reports whether both the `prado-http2` package and the `libnghttp2` library are present. When either is missing the server still runs — it just serves **HTTP/1.1 only**, and rejects connections that arrive speaking HTTP/2.

## Installation

```sh
composer require belisoful/prado-websocket
```

PRADO 4.4 is not yet released, so the extension depends on its development branch (`^4.4@dev`). Composer applies stability flags only from the root project, so allow it there, for example with `"minimum-stability": "dev"` and `"prefer-stable": true`, or by requiring `pradosoft/prado:^4.4@dev` yourself.

Release notes and upgrade steps between versions are in [CHANGELOG.md](CHANGELOG.md).

## What it provides

| Class | Role |
|---|---|
| `TWebSocketFrame` | An RFC 6455 frame: opcode, payload, FIN, RSV bits, with `text()`/`binary()`/`ping()`/`pong()`/`close()`/`continuation()` factories |
| `TWebSocketFrameCodec` | The wire codec: `encode()`, blocking `decode()` (from a stream), and non-blocking `tryDecode()` (from a buffer), with masking |
| `TWebSocketOpcode` / `TWebSocketCloseCode` | Opcode and close-code enumerations, with `isControl()` / `isSendable()` |
| `TWebSocketHandshake` | The HTTP/1.1 opening handshake: accept-key computation, request/response building, and end-to-end stream drivers (`acceptConnection()`, `openConnection()`) |
| `TWebSocketConnection` | A connection: `send()`/`sendBinary()`/`ping()`/`pong()`/`close()`, blocking `receive()`/`receiveMessage()`/`receiveFrame()`, non-blocking `feed()`, and `onPing`/`onPong`/`onClose` events |
| `TWebSocketMessage` | The `Stringable` message model (opcode + payload), with `getIsText()`/`getIsBinary()` |
| `TWebSocketException` | A protocol/handshake failure carrying a `CloseCode`; extends `TIOException` |
| `IWebSocketExtension` / `IWebSocketExtensionNegotiator` | The RFC 6455 extension seam: an extension transforms message payloads on the wire; its negotiator agrees terms during the handshake |
| `TPermessageDeflateExtension` / `TPermessageDeflateNegotiator` | RFC 7692 permessage-deflate — negotiated, DoS-bounded message compression |
| `IWebSocketProtocol` | The protocol-stack seam: turns a transport into the WebSocket logical streams it carries |
| `THttp1WebSocketProtocol` | The RFC 6455 stack — one WebSocket per connection |
| `THttp2WebSocketProtocol` | The RFC 8441 stack — many WebSockets over one HTTP/2 connection (uses `prado-http2`) |
| `TWebSocketServer` | The standalone server: a `select()` event loop fanning out across many connections, auto-selecting H1/H2 |
| `IWebSocketHandler` | The connection/message contract the server dispatches through (`onOpen`/`onMessage`/`onClose`/`onError`) |
| `TWebSocketHandler` | The standalone handler: a `TComponent` raising the lifecycle events, used by `TWebSocketServer` |
| `Prado\Web\Services\TWebSocketService` | A `TService` adapting the `IWebSocketHandler` role to a SAPI upgrade request in the PRADO service pipeline |
| `TWebSocketModule` | The server module (a PRADO `TSocketServerModule`): `prado-cli websocket/serve` runs a configured `TWebSocketServer`, and the module makes it one node of a cluster over an `IWebSocketBackplane` (the `websocket_*` error codes and Prado3 class names are registered by Composer from `extra.prado`) |
| `TWebSocketServerAction` | The `websocket/serve` shell action the module registers |
| `TWebSocketCluster` | The cluster coordinator: `subscribe()`/`publish()`/`broadcast()`/`sendToClient()`/`presence()` fanning across nodes |
| `IWebSocketBackplane` | The transport seam a cluster relays through; `TWebSocketEnvelope` is its unit of exchange |
| `TNullBackplane` | Single-node no-op backplane (the default) |
| `TFileBackplane` | Shared-directory backplane for one host or a shared filesystem (dev/small clusters); owner-only spool |
| `TRedisBackplane` | Redis pub/sub + presence backplane for multi-host scaling (needs `ext-redis`) |
| `TMeshBackplane` | Peer-to-peer gossip backplane over server-to-server WebSocket links; shared-secret authenticated |
| `IWebSocketClusterAware` | A handler the module hands its cluster to |
| `TWebSocketPubSubHandler` | Serves the `prado.pubsub.v1` subprotocol: browser subscribe/publish/send/call over JSON, routed through the cluster |
| `TWebSocketPubSubEventParameter` / `TWebSocketPubSubException` | The pub/sub request event parameter, and the rejection carrying the reply code |
| `prado-pubsub.js` | The dependency-free browser client of `prado.pubsub.v1` |

## Architecture

```
   TWebSocketModule / TWebSocketCluster ──► IWebSocketBackplane  (Null / File / Redis / Mesh)
                                │                    (fan a publish/presence across nodes)
                         TWebSocketServer  (TSocketReactor event loop; non-blocking handshake, auto-selects)
                                │
              ┌─────────────────┴─────────────────┐
   THttp1WebSocketProtocol               THttp2WebSocketProtocol  ──► prado-http2 (TH2Session)
   (RFC 6455 Upgrade, 1 WS/conn)         (RFC 8441 Extended CONNECT, N WS/conn)
                                │
                       TWebSocketConnection   (send/receive/control; blocking + feed())
                                │
              IWebSocketExtension pipeline   (RFC 7692 permessage-deflate, …)
                                │
              TWebSocketFrameCodec ◄──► TWebSocketFrame / Opcode / CloseCode
                                │
                       IWebSocketHandler     (onOpen / onMessage / onClose / onError)
```

The layers stack cleanly:

- **Frames** — `TWebSocketFrame` + `TWebSocketFrameCodec` are the RFC 6455 model and wire format (FIN/RSV/opcode, 7/16/64-bit lengths, client masking). `decode()` reads one frame from a stream (blocking); `tryDecode()` parses one frame from an in-memory buffer (non-blocking, returns `null` until a full frame is present).
- **Connection** — `TWebSocketConnection` reassembles fragments, auto-answers Pings, and completes the close handshake. It offers a **blocking** path (`receive()` for the next message) and a **non-blocking** path (`feed()` takes the bytes just read and returns the complete messages) for an event loop.
- **Protocol stacks** — `IWebSocketProtocol` is the seam. HTTP/1.1 yields one connection per socket; HTTP/2 multiplexes many over one. The server picks the stack from the connection's first bytes (the HTTP/2 preface starts with `PRI `), gathered without blocking.
- **Server & handler** — `TWebSocketServer` owns the socket and pumps connections, dispatching through an `IWebSocketHandler` that raises lifecycle events with the connection as sender. `TWebSocketHandler` is the standalone handler (a `TComponent`); `TWebSocketService` is a `TService` implementing the same role for web-app request routing.

## Usage

### Standalone server (auto HTTP/1.1 + HTTP/2)

```php
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;

$handler = new TWebSocketHandler();
$handler->attachEventHandler('onOpen', function ($connection) {
    // a client connected (over HTTP/1.1 or an HTTP/2 stream)
});
$handler->attachEventHandler('onMessage', function ($connection, $message) {
    // $message is a TWebSocketMessage: getPayload(), getOpcode(), getIsText()/getIsBinary(); it stringifies to the payload
    $message->getIsBinary()
        ? $connection->sendBinary($message->getPayload())
        : $connection->send("echo: $message");    // reply on the same connection
});
$handler->attachEventHandler('onClose', function ($connection) { /* gone */ });
$handler->attachEventHandler('onError', function ($connection, $error) { /* protocol error */ });

$server = TWebSocketServer::bind('tcp://0.0.0.0:8080');
$server->setHandler($handler);                    // required (HTTP/2 dispatches through it)
$server->serve();                                 // TSocketReactor-driven loop; one process, many clients
```

The loop is PRADO's `TSocketReactor`: the listener, every connection and the cluster backplane's sockets are multiplexed through one `select()`, transports are watched for writes only while output is queued, and the handshake, close and idle deadlines run as reactor timers. Pass your own reactor with `setReactor()` to run other sources in the same loop; `serveOnce()` runs one tick for tests.

### As a daemon from prado-cli

`TWebSocketModule` is a PRADO `TSocketServerModule`: configure it in the application and run it with the shell:

```xml
<modules>
    <module id="websockets" class="Prado\IO\Socket\WebSocket\TWebSocketModule"
        Address="0.0.0.0" Port="8080" HandlerClass="Application\Chat\ChatHandler"
        Subprotocols="chat" IdleTimeout="60" PermessageDeflate="true" />
</modules>
```

```sh
php protected/prado-cli.php websocket/serve            # or: --port 9000 --address 127.0.0.1
```

The module binds `Endpoint` (or `Scheme`/`Address`/`Port`; `tls://` with `SocketOptions` for the certificate), creates the `HandlerClass` handler (a `TWebSocketHandler` by default) and re-raises its `onOpen`/`onMessage`/`onClose`/`onError` as module events, applies `Subprotocols`, `Origins`, `AllowedHosts`, `PermessageDeflate` and the limits (`MaxMessageSize`, `HandshakeTimeout`, `IdleTimeout`, `CloseTimeout`, `MaxConnections`), and serves until SIGTERM or CTRL-C. Running it is gated by PRADO's `socket_server` permission.

Each accepted connection stays non-blocking while its opening bytes arrive: the HTTP/2 preface starts an HTTP/2 session (one socket, many multiplexed WebSockets); otherwise the RFC 6455 upgrade handshake runs. A connection that has not completed its handshake within `HandshakeTimeout` is dropped, and the loop's wait is bounded by the nearest handshake deadline, idle scan, or cluster tick, so idle reaping runs on a quiet server too. Either way, complete messages dispatch to the handler, and `onConnection` is raised on the server per ready `TWebSocketConnection`.

### As a PRADO service (web app routing)

`TWebSocketService` is the `websocket` service, selected by an upgrade request via
`Prado\Web\Behaviors\TRequestConnectionUpgrade` (which routes `Connection: Upgrade` / `Upgrade: websocket` to it). Configure it alongside the bootstrap module:

```xml
<modules>
    <module id="websockets" class="Prado\IO\Socket\WebSocket\TWebSocketModule" />
</modules>
<services>
    <service id="websocket" class="Prado\Web\Services\TWebSocketService" />
</services>
```

A web SAPI (PHP-FPM, mod_php) cannot hand its socket to PHP, so the service cannot complete an upgrade there: it validates the request and answers 400 (not an upgrade), 426 with `Sec-WebSocket-Version: 13` (wrong version), or 501 (a valid upgrade the SAPI cannot serve), so a client gets a definite refusal instead of an empty 200. Serve WebSockets with the standalone `TWebSocketServer`; the service runs a connection only when one is injected with `setConnection()` (a bridge or a test).

### Client connection

```php
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;

$socket = TSocketStream::connect('tcp://example.com:8080', 5.0);
$client = TWebSocketConnection::connect($socket, 'example.com', '/chat');  // RFC 6455 handshake
$client->send('hello');
$reply = $client->receive();                       // blocking; null on close/EOF
$client->close(1000);
```

### Frames and codec directly

```php
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;

$bytes = TWebSocketFrameCodec::encode(TWebSocketFrame::text('hi'));   // server frame (unmasked)
$frame = TWebSocketFrameCodec::tryDecode($buffer);                    // null until a whole frame
```

### Subprotocols and extensions

```php
$server->setSubprotocols(['chat', 'superchat']);   // offered in order; the agreed one is echoed in Sec-WebSocket-Protocol
// per connection: $connection->getSubprotocol()   // the negotiated subprotocol, or null
```

Extensions are pluggable through `IWebSocketExtension` (transforms payloads on the wire) and `IWebSocketExtensionNegotiator` (agrees terms during the handshake). Offer them on the server in preference order:

```php
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;

$server->setExtensions([new TPermessageDeflateNegotiator()]);   // offer RFC 7692 permessage-deflate
```

Negotiation runs on every accept path: the `serveOnce()` loop, the synchronous `serveConnection()`, and HTTP/2 streams. The protocol stacks carry the same settings themselves (`THttp1WebSocketProtocol` and `THttp2WebSocketProtocol` have `Subprotocols`/`Extensions` properties, and the HTTP/1.1 stack a `HandshakeTimeout`), and a protocol's `serve()` callback receives `(StreamInterface $stream, array $handshake)` with the negotiated `subprotocol` and `extensions`. Header fields split across lines (RFC 7230) are combined before negotiation. Close codes 1000–1003, 1007–1014 and 3000–4999 are valid to send and receive.

## Compression (RFC 7692 permessage-deflate)

`TPermessageDeflateExtension` compresses message payloads with DEFLATE when both peers negotiate it; it is transparent to `onMessage`/`receive()`. Enable it by offering `TPermessageDeflateNegotiator` (above); the negotiator's constructor tunes the context-takeover and window-bits parameters, and inflation is **bounded**: each inflate step is sized against the remaining output budget, so a compression-bomb frame cannot materialize more than about 1 KiB beyond `MaxMessageSize`. A truncated compressed message closes with 1007. Offers and responses are checked per RFC 7692 §7.1: a malformed or duplicated parameter declines the offer (falling through to the client's next one), and a client is bound by the parameters it offered even when the server omits them. It needs `ext-zlib`.

## Hardening and limits

`TWebSocketServer` exposes the operational limits and origin checks a public deployment needs. All are optional; the defaults are safe but permissive on the network-policy axes (empty allow-lists accept any origin/host).

| Property | Default | Effect |
|---|---|---|
| `setMaxMessageSize($bytes)` | 10 MiB | Caps an inbound frame/message; a larger one is rejected (`MessageTooBig`) before buffering |
| `setHandshakeTimeout($seconds)` | 10.0 | Deadline for the opening handshake; a silent or dribbling client is dropped without ever blocking the loop |
| `setIdleTimeout($seconds)` | 0 (off) | Pings, then reaps, a connection idle this long; runs on a quiet server too |
| `setCloseTimeout($seconds)` | 5.0 | After the server sends Close, how long it waits for the peer's Close before ending the session; the queued Close is drained first |
| `setMaxConnections($n)` | 0 (unlimited) | Bounds the load: every transport the server holds (sessions, pending handshakes, endpoint links, app-held connections) plus HTTP/2 streams; a further connection is accepted and shed with 503 |
| `setReactor($reactor)` | own | The `TSocketReactor` the loop runs on; supply one to multiplex other sources or timers with the server |
| `setOrigins([...])` | `[]` (any) | Allowed `Origin` values; a disallowed origin is refused with 403 before upgrading |
| `setAllowedHosts([...])` | `[]` (any) | Allowed `Host` values; a disallowed host is refused with 400 |

These apply on both the HTTP/1.1 and HTTP/2 paths. Binding `tls://host:port` (with `ssl` context options for the certificate) runs each accepted socket's TLS handshake without blocking, under the handshake deadline; an ALPN `h2` selects HTTP/2 without a preface. Every refused upgrade, shed connection, deadline, idle reap and swallowed handler error is logged through `Prado::log()` under the server's class.

## Clustering (multi-node)

Several server processes (each a `websocket/serve` daemon, or a `TWebSocketServer` handed the module's cluster with `prepareServer()`) act as one logical endpoint by relaying through an `IWebSocketBackplane`, so `publish()`/`broadcast()`/`sendToClient()` and presence on any node reach clients on every node. Each takes an optional `$binary` flag to deliver a Binary frame; any byte string crosses every backplane intact. A client whose send fails is closed and logged, and never blocks delivery to the others. Configure `TWebSocketModule` with a `<backplane>` child; without one it runs a single node on `TNullBackplane`.

```xml
<modules>
    <module id="websockets" class="Prado\IO\Socket\WebSocket\TWebSocketModule" NodeId="edge-1">
        <backplane class="Prado\IO\Socket\WebSocket\Cluster\TRedisBackplane" Host="127.0.0.1" Port="6379" />
    </module>
</modules>
<services>
    <service id="websocket" class="Prado\Web\Services\TWebSocketService" />
</services>
```

Backplane choices:

- **`TNullBackplane`** — single node, no relay (the default).
- **`TFileBackplane`** — a shared directory (`Directory`); for one host or a shared filesystem (dev, tests, small clusters). The spool is created owner-only and refused if another user owns it, any other user can access it, or it is a symlink. A crashed node's presence files go stale after `PresenceTtl` and its clients are dropped on every node.
- **`TRedisBackplane`** — per-node inbox lists plus a presence registry (`Host`/`Port`/`Password`/`Database`/`Prefix`/`InboxLimit`); the driver for multi-host scaling. Needs `ext-redis`. Redis pub/sub is not used (phpredis subscribe blocks); a node polls its inbox each tick. A dropped connection is retried every 5 s, and a reconnect re-declares the node's channel interest and presence from local state; a node that restarts under the same `NodeId` purges its previous incarnation's leftovers.
- **`TMeshBackplane`** — peer-to-peer gossip over server-to-server WebSocket links (`Peers`/`Advertise`), with no shared service. A peer joins only by proving the shared `Secret` (required; `open()` refuses without one) — a handshake HMAC plus a *mutual* post-upgrade nonce challenge bound to the answering node's id, so each side proves the secret to the other and shows no state until it has, and a challenge cannot be reflected. A relay through a third node that holds the secret is not prevented, so prefer a `tls://` transport on any untrusted network. A node unheard for `NodeTtl` is declared down: its clients leave the presence mirror and its link is dropped so it is re-dialed when it returns; unlinked seed peers are re-dialed once per TTL.

## Pub/sub for browsers (`prado.pubsub.v1`)

A browser needs no library to open a WebSocket, but the cluster's `subscribe()`/`publish()` are server-side calls. `TWebSocketPubSubHandler` defines a small JSON subprotocol over them, and `prado-pubsub.js` is its browser client.

Each message is one JSON object with a `type`. A request with an `id` gets exactly one `ack` (with `data` for a call) or `error` (`code`, `message`); a request without one is answered only when it fails.

| Client → server | Fields | Server → client | Fields |
|---|---|---|---|
| `subscribe` / `unsubscribe` | `channel` | `welcome` | `clientId`, `heartbeat` |
| `publish` | `channel`, `data` | `message` | `data`, `channel`?, `from`? |
| `send` | `to`, `data` | `ack` | `id`, `data`? |
| `call` | `method`, `params` | `error` | `id`?, `code`, `message` |
| `ping` | | `pong` | |

Channels match `[A-Za-z0-9_.:/-]{1,128}`. Error codes are `bad_request`, `unknown_type`, `forbidden`, `limit`, `not_found` and `internal`. Delivery is at-most-once: a client that is reconnecting misses what is published meanwhile.

```xml
<module id="websockets" class="Prado\IO\Socket\WebSocket\TWebSocketModule" Port="8080"
    Subprotocols="prado.pubsub.v1" IdleTimeout="60"
    HandlerClass="Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubHandler"
    OnPublish="Application.Chat.authorizePublish" OnCall="Application.Chat.call" />
```

Authorization runs through events, each with a `TWebSocketPubSubEventParameter`:

- `onSubscribe` → allowed unless a handler calls `setAllowed(false)`.
- `onPublish` / `onSend` → denied unless `AllowClientPublish` / `AllowClientSend` is set or a handler allows it; a handler may also `setData()`.
- `onCall` → a handler answers with `setResult()`, or throws a `TWebSocketPubSubException`; an unanswered call is `not_found`.
- `onOpen` is raised before the `welcome`, so it can authenticate the client (close with 4401 or 4403 to stop the browser from reconnecting) or `subscribe()` it to its own channels.

`MaxSubscriptions` (default 64) bounds each client, and `Heartbeat` (default 25 s; keep it below `IdleTimeout`) sets the client ping interval. Application code reaches clients with `publish()`, `sendTo()` and `broadcast()` on the handler, or with `$module->publish($channel, TWebSocketPubSubHandler::encodeMessage($data, $channel))`. The module gives the handler its cluster; a bare `TWebSocketServer` and the handler share one with `setCluster()`.

```php
$page->getClientScript()->registerScriptFile('prado-pubsub',
    $page->publishFilePath(TWebSocketPubSubHandler::getClientScriptPath()));
```

```js
const ps = new Prado.WebSocket.PubSub('wss://example.com/ws');
ps.on('open', ({ clientId }) => console.log('connected as', clientId));
const leave = await ps.subscribe('room:42', (data, frame) => render(data, frame.from));
await ps.publish('room:42', { text: 'hi' });          // rejects with e.code === 'forbidden' when denied
const rows = await ps.call('chat.history', { room: 42 });
leave();
```

[`examples/chat`](examples/chat) is a runnable multi-room chat on this layer: rooms, history, whispers, and reconnection, run both standalone and as a PRADO application.

The client reconnects with exponential backoff and jitter (`minDelay`, `maxDelay`), re-subscribes, holds requests made while offline (`maxQueue`), times requests out (`requestTimeout`), and drops a connection silent for twice the heartbeat. Close codes 1002, 1003, 1007 to 1010, 4401 and 4403 end reconnection. A publish, send or call in flight when the connection drops rejects with `disconnected` and is not resent. Pub/sub runs on the standalone server; `TWebSocketService` does not serve it.

## HTTP/2 multiplexing (RFC 8441)

HTTP/2 is an optional capability, active only when the `prado-http2` package and `libnghttp2` are installed (see Requirements). When present, the server recognizes the HTTP/2 connection preface in a connection's first bytes and runs an HTTP/2 session; when absent, `isHttp2Available()` is false and HTTP/2 connections are declined.

Over HTTP/2, each WebSocket is an Extended CONNECT (`:method` CONNECT, `:protocol` websocket) on its own stream, and RFC 6455 frames flow as that stream's DATA. The HTTP/2 framing, HPACK, and per-stream flow control are handled by `libnghttp2` through the `prado-http2` extension; `THttp2WebSocketProtocol` bridges each stream to a `TWebSocketConnection` via the non-blocking `feed()` path, so many WebSockets share one socket. The server advertises `SETTINGS_ENABLE_CONNECT_PROTOCOL` and accepts a CONNECT with `:status` 200.

The HTTP/1.1 path never references `prado-http2`: the dependency is loaded lazily, only when an HTTP/2 connection is actually served, so HTTP/1.1-only deployments need neither the package nor `ext-ffi`/`libnghttp2`.

## Limitations

- **Web SAPIs cannot host WebSockets.** Under PHP-FPM/mod_php the web server owns the socket and FastCGI cannot tunnel the upgrade to PHP. Run the standalone `TWebSocketServer` in its own process. (The PRADO `websocket` service is the dispatch target; the socket is supplied by the server.)
- **HTTP/3 (RFC 9220) is out of scope** — QUIC needs TLS key hooks PHP does not expose.
- **TLS** (`wss://`, `h2`) is terminated on the socket; HTTP/2 over TLS needs ALPN negotiating `h2` before the bytes reach the server.
- **`MaxConnections` and HTTP/2**: streams count when their session is accepted; a new stream on an existing session past the cap is not refused.

## Development

```sh
composer install
composer unittest        # phpunit --testsuite unit
composer fix             # php-cs-fixer on src/ and tests/
composer stan            # phpstan (level 3, PHP 8.1 – 8.5)
composer fulltest        # fix, stan, unittest in order
composer coverage        # unit tests with a text coverage summary (needs Xdebug)
composer coverage-html   # HTML coverage report in build/coverage
```

Unit tests live under `tests/unit/` in the `Prado\Test\Unit\` namespace (Composer `autoload-dev`), mirroring the directory. CI runs the suite against the PRADO 4.4 development branch and the `prado-http2` main branch on PHP 8.1, 8.2, 8.3, 8.4, and 8.5; the Autobahn|TestSuite server-compliance run and the three Playwright browser jobs run on PHP 8.4.

Tests cover the codec (round-trips, masking, fragmentation, control-frame rules), the handshake (RFC 6455 accept-key vector), the connection (blocking and `feed()` paths over socket pairs), the server (HTTP/1.1 over a real socket and HTTP/2 auto-selection), and the RFC 8441 round-trip end to end. HTTP/2 tests skip cleanly where `libnghttp2` is absent.

### Browser client tests (Playwright)

A Playwright suite drives a **real browser `WebSocket`** (Chromium, Firefox, and WebKit) against the standalone server, exercising the RFC 6455 handshake and framing end to end — the runtime coverage the PHP unit and Autobahn suites cannot give. The specs echo text, multibyte UTF-8, and binary, round-trip a 256 KiB message, check ordering, negotiate a subprotocol, and interoperate with permessage-deflate. The pub/sub specs drive `prado-pubsub.js` against `TWebSocketPubSubHandler`: channel delivery, calls, error codes, a fatal close, reconnection after a server restart, and heartbeat detection of a stalled server. The chat-example specs run both versions of [`examples/chat`](examples/chat) in two browser pages.

```sh
npm install                                  # or: bun install
npx playwright install                       # download the browser builds (or: bunx playwright install)
npx playwright test                          # all three engines  (or: bunx playwright test)
npx playwright test --project=chromium       # one engine
HEADLESS=false npx playwright test           # watch it run
```

The specs live in `tests/playwright/`; a small PHP echo server ([`ws-server.php`](tests/playwright/ws-server.php), or pub/sub with `WS_PUBSUB=1`) is spawned per run, and a static page server gives the browser a real HTTP origin. Nothing here is required for the PHP suite — it is an optional, browser-only layer.

## License

BSD-3-Clause. See [LICENSE](LICENSE).
