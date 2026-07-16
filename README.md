# PRADO WebSockets Extension

WebSockets for the [PRADO PHP Framework](https://github.com/pradosoft/prado) (version 4.4+), implemented as a PRADO 4 extension:

- **[RFC 6455](https://www.rfc-editor.org/rfc/rfc6455.html) over HTTP/1.1** — the classic `Upgrade` handshake, one WebSocket per connection. The base capability; needs only PHP and PRADO.
- **[RFC 8441](https://www.rfc-editor.org/rfc/rfc8441.html) over HTTP/2** — Extended CONNECT, many WebSockets multiplexed over one connection. **Optional**: enabled only when the [`prado-http2`](https://github.com/pradosoft/prado-http2) extension and the system `libnghttp2` are present.
- **[RFC 7692](https://www.rfc-editor.org/rfc/rfc7692.html) permessage-deflate** — negotiated per-message compression, layered on either transport. **Optional**: enabled by offering the extension; needs `ext-zlib`.

A **clustering** layer (`TWebSocketModule` + pluggable backplanes) additionally lets many server processes act as one logical endpoint, so a publish or presence change on any node reaches clients on every node.

The standalone `TWebSocketServer` owns its listening socket end to end, so it completes the upgrade and streams frames in its own process — and **auto-selects HTTP/1.1 or HTTP/2 per connection** by peeking the first bytes (it serves HTTP/1.1 only when HTTP/2 is unavailable). A typical web SAPI (PHP-FPM, mod_php) cannot do WebSockets: the web server owns the socket and FastCGI cannot hand it to PHP. Run this as a long-lived server process instead.

## Requirements

| Requirement | Scope | Purpose |
|---|---|---|
| PHP 8.1 or higher | required | The only hard requirement; HTTP/1.1 WebSockets need nothing more |
| PRADO Framework `^4.4` | dev | `TSocketServer`, `TSocketStream`, the `TStream` IO layer, `TComponent`/`TService`/`TModule` |
| `belisoful/prado-http2` `^1.0` | suggested | The HTTP/2 (RFC 8441) stack; without it the server serves HTTP/1.1 only |
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
composer require belisoful/prado-websockets
```

## What it provides

| Class | Role |
|---|---|
| `TWebSocketFrame` | An RFC 6455 frame: opcode, payload, FIN, RSV bits, with `text()`/`binary()`/`ping()`/`pong()`/`close()`/`continuation()` factories |
| `TWebSocketFrameCodec` | The wire codec: `encode()`, blocking `decode()` (from a stream), and non-blocking `tryDecode()` (from a buffer), with masking |
| `TWebSocketOpcode` / `TWebSocketCloseCode` | Opcode and close-code enumerations, with `isControl()` / `isSendable()` |
| `TWebSocketHandshake` | The HTTP/1.1 opening handshake: accept-key computation, request/response building, and end-to-end stream drivers (`acceptConnection()`, `openConnection()`) |
| `TWebSocketConnection` | A connection: `send()`/`sendBinary()`/`ping()`/`pong()`/`close()`, blocking `receive()`/`receiveFrame()`, non-blocking `feed()`, and `onPing`/`onPong`/`onClose` events |
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
| `TWebSocketModule` | The cluster module, making the server one node of a cluster over an `IWebSocketBackplane` (the `websocket_*` error codes and Prado3 class names are registered by Composer from `extra.prado`) |
| `TWebSocketCluster` | The cluster coordinator: `subscribe()`/`publish()`/`broadcast()`/`sendToClient()`/`presence()` fanning across nodes |
| `IWebSocketBackplane` | The transport seam a cluster relays through; `TWebSocketEnvelope` is its unit of exchange |
| `TNullBackplane` | Single-node no-op backplane (the default) |
| `TFileBackplane` | Shared-directory backplane for one host or a shared filesystem (dev/small clusters); owner-only spool |
| `TRedisBackplane` | Redis pub/sub + presence backplane for multi-host scaling (needs `ext-redis`) |
| `TMeshBackplane` | Peer-to-peer gossip backplane over server-to-server WebSocket links; shared-secret authenticated |

## Architecture

```
   TWebSocketModule / TWebSocketCluster ──► IWebSocketBackplane  (Null / File / Redis / Mesh)
                                │                    (fan a publish/presence across nodes)
                         TWebSocketServer  (select() event loop; peeks preface, auto-selects)
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
- **Protocol stacks** — `IWebSocketProtocol` is the seam. HTTP/1.1 yields one connection per socket; HTTP/2 multiplexes many over one. The server picks the stack by peeking the connection's first bytes (the HTTP/2 preface starts with `PRI `).
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
    $connection->send("echo: $message");          // reply on the same connection
});
$handler->attachEventHandler('onClose', function ($connection) { /* gone */ });
$handler->attachEventHandler('onError', function ($connection, $error) { /* protocol error */ });

$server = TWebSocketServer::bind('tcp://0.0.0.0:8080');
$server->setHandler($handler);                    // required (HTTP/2 dispatches through it)
$server->serve();                                 // select()-driven loop; one process, many clients
```

On each accepted connection the server peeks the first bytes: the HTTP/2 preface starts an HTTP/2 session (one socket, many multiplexed WebSockets); otherwise the RFC 6455 upgrade handshake runs. Either way, complete messages dispatch to the handler, and `onConnection` is raised on the server per ready `TWebSocketConnection`.

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

## Compression (RFC 7692 permessage-deflate)

`TPermessageDeflateExtension` compresses message payloads with DEFLATE when both peers negotiate it; it is transparent to `onMessage`/`receive()`. Enable it by offering `TPermessageDeflateNegotiator` (above); the negotiator's constructor tunes the context-takeover and window-bits parameters, and inflation is **bounded** (chunked, output-capped) so a compression-bomb frame cannot exhaust memory. It needs `ext-zlib`.

## Hardening and limits

`TWebSocketServer` exposes the operational limits and origin checks a public deployment needs. All are optional; the defaults are safe but permissive on the network-policy axes (empty allow-lists accept any origin/host).

| Property | Default | Effect |
|---|---|---|
| `setMaxMessageSize($bytes)` | 10 MiB | Caps an inbound frame/message; a larger one is rejected (`MessageTooBig`) before buffering |
| `setHandshakeTimeout($seconds)` | 10.0 | Deadline for the opening handshake; a slow client is dropped |
| `setIdleTimeout($seconds)` | 0 (off) | Pings, then reaps, a connection idle this long |
| `setMaxConnections($n)` | 0 (unlimited) | Concurrent-session cap; a further connection is accepted and shed with 503 |
| `setOrigins([...])` | `[]` (any) | Allowed `Origin` values; a disallowed origin is refused with 403 before upgrading |
| `setAllowedHosts([...])` | `[]` (any) | Allowed `Host` values; a disallowed host is refused with 400 |

These apply on both the HTTP/1.1 and HTTP/2 paths.

## Clustering (multi-node)

Several server processes act as one logical endpoint by relaying through an `IWebSocketBackplane`, so `publish()`/`broadcast()`/`sendToClient()` and presence on any node reach clients on every node. Configure `TWebSocketModule` with a `<backplane>` child; without one it runs a single node on `TNullBackplane`.

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
- **`TFileBackplane`** — a shared directory (`Directory`); for one host or a shared filesystem (dev, tests, small clusters). The spool is created owner-only and refused if another user owns it or it is a symlink.
- **`TRedisBackplane`** — Redis pub/sub + a presence registry (`Host`/`Port`/`Password`/`Database`/`Prefix`); the driver for multi-host scaling. Needs `ext-redis`.
- **`TMeshBackplane`** — peer-to-peer gossip over server-to-server WebSocket links (`Peers`/`Advertise`), with no shared service. A peer joins only by proving a shared `Secret` — a handshake HMAC plus a *mutual* post-upgrade nonce challenge, so each side proves the secret to the other and shows no state until it has; set it and prefer a `tls://` transport on any untrusted network.

## HTTP/2 multiplexing (RFC 8441)

HTTP/2 is an optional capability, active only when the `prado-http2` package and `libnghttp2` are installed (see Requirements). When present, the server peeks the HTTP/2 connection preface on accept and runs an HTTP/2 session; when absent, `isHttp2Available()` is false and HTTP/2 connections are declined.

Over HTTP/2, each WebSocket is an Extended CONNECT (`:method` CONNECT, `:protocol` websocket) on its own stream, and RFC 6455 frames flow as that stream's DATA. The HTTP/2 framing, HPACK, and per-stream flow control are handled by `libnghttp2` through the `prado-http2` extension; `THttp2WebSocketProtocol` bridges each stream to a `TWebSocketConnection` via the non-blocking `feed()` path, so many WebSockets share one socket. The server advertises `SETTINGS_ENABLE_CONNECT_PROTOCOL` and accepts a CONNECT with `:status` 200.

The HTTP/1.1 path never references `prado-http2`: the dependency is loaded lazily, only when an HTTP/2 connection is actually served, so HTTP/1.1-only deployments need neither the package nor `ext-ffi`/`libnghttp2`.

## Limitations

- **Web SAPIs cannot host WebSockets.** Under PHP-FPM/mod_php the web server owns the socket and FastCGI cannot tunnel the upgrade to PHP. Run the standalone `TWebSocketServer` in its own process. (The PRADO `websocket` service is the dispatch target; the socket is supplied by the server.)
- **HTTP/3 (RFC 9220) is out of scope** — QUIC needs TLS key hooks PHP does not expose.
- **TLS** (`wss://`, `h2`) is terminated on the socket; HTTP/2 over TLS needs ALPN negotiating `h2` before the bytes reach the server.

## Development

```sh
composer install
vendor/bin/phpunit --testsuite unit                  # tests
vendor/bin/php-cs-fixer fix --dry-run src/           # code style
vendor/bin/phpstan analyse src/ --memory-limit=512M  # static analysis
```

Tests cover the codec (round-trips, masking, fragmentation, control-frame rules), the handshake (RFC 6455 accept-key vector), the connection (blocking and `feed()` paths over socket pairs), the server (HTTP/1.1 over a real socket and HTTP/2 auto-selection), and the RFC 8441 round-trip end to end. HTTP/2 tests skip cleanly where `libnghttp2` is absent.

### Browser client tests (Playwright)

A Playwright suite drives a **real browser `WebSocket`** (Chromium, Firefox, and WebKit) against the standalone server, exercising the RFC 6455 handshake and framing end to end — the runtime coverage the PHP unit and Autobahn suites cannot give. The specs echo text, multibyte UTF-8, and binary, round-trip a 256 KiB message, check ordering, negotiate a subprotocol, and interoperate with permessage-deflate.

```sh
npm install                                  # or: bun install
npx playwright install                       # download the browser builds (or: bunx playwright install)
npx playwright test                          # all three engines  (or: bunx playwright test)
npx playwright test --project=chromium       # one engine
HEADLESS=false npx playwright test           # watch it run
```

The specs live in `tests/playwright/`; a small PHP echo server ([`ws-server.php`](tests/playwright/ws-server.php)) is spawned per run, and a static page server gives the browser a real HTTP origin. Nothing here is required for the PHP suite — it is an optional, browser-only layer.

## License

BSD-3-Clause. See [LICENSE](LICENSE).
