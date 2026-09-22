# Changelog

All notable changes to `belisoful/prado-websocket` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.1.0] - 2026-09-22

This release resyncs the extension with PRADO 4.4 development HEAD and lands a full audit and hardening pass. It changes some public API; see [Upgrading from 1.0.x](#upgrading-from-10x).

### Added
- `TWebSocketModule` is a `TSocketServerModule`: `prado-cli websocket/serve` (`TWebSocketServerAction`) runs a configured `TWebSocketServer` as a daemon with signal handling and the `socket_server` permission gate. The module configures the server through `ServerClass`, `Handler`/`HandlerClass`, `Subprotocols`, `Origins`, `AllowedHosts`, `PermessageDeflate`, `MaxMessageSize`, `MaxSendBufferBytes`, `HandshakeTimeout`, `IdleTimeout`, `CloseTimeout` and `MaxConnections`, and re-raises the handler's events as `onOpen`, `onMessage`, `onClose` and `onError`.
- `TWebSocketServer` runs on PRADO's `TSocketReactor`: the listener, sessions and cluster sources are reactor sources, writes are armed per transport, and the handshake, close and idle deadlines are reactor timers.
- TLS: `TWebSocketServer::bind('tls://…')` runs each accepted socket's TLS handshake non-blocking under the handshake deadline; ALPN `h2` selects HTTP/2 without a preface (`getIsSecure()`).
- `CloseTimeout` (default 5 s): a closing session drains the server's Close frame and is ended at the deadline.
- `MaxSendBufferBytes` on `TWebSocketServer`, `THttp2WebSocketProtocol` and `TWebSocketModule`, applied to every connection the server creates.
- `TWebSocketServer::getLoad()`, which `MaxConnections` bounds: every session, pending handshake, endpoint link and HTTP/2 stream.
- `TWebSocketMessage`: the handler's `onMessage` event carries one, with `getPayload()`, `getOpcode()`, `getIsText()` and `getIsBinary()`. It converts to its payload when used as a string. `TWebSocketConnection::receiveMessage()` returns one.
- Binary cluster delivery: `publish()`, `broadcast()` and `sendToClient()` on `TWebSocketCluster` and `TWebSocketModule` take a `$binary` flag. `TWebSocketEnvelope` carries any byte string intact (`getIsBinary()`).
- `TRedisBackplane::InboxLimit` (default 10000) and pipelined delivery; `IWebSocketCluster::getLocalPresence()`, which the Redis backplane uses to re-declare presence on reconnect.
- Close codes 1012 to 1014 (`TWebSocketCloseCode::ServiceRestart`, `TryAgainLater`, `BadGateway`).
- `THttp1WebSocketProtocol` and `THttp2WebSocketProtocol` negotiate `Subprotocols` and `Extensions` on every accept path; `THttp1WebSocketProtocol` also takes a `HandshakeTimeout`.
- `TWebSocketHandshake::isSupportedVersion()`; `readHandshake()`, `receiveRequest()`, `acceptConnection()` and `openConnection()` take an optional `IClock`.
- `TPermessageDeflateExtension::ensureLevel()` with `MIN_LEVEL`/`MAX_LEVEL` validation.
- `TWebSocketService` answers a SAPI request it cannot upgrade with 400 (not an upgrade), 426 with `Sec-WebSocket-Version: 13` (wrong version), or 501 (a valid upgrade the SAPI cannot serve), instead of an empty 200.
- Logging through `Prado::log()` for refused upgrades, shed connections, deadlines, idle reaps, TLS failures, swallowed handler errors, peer drops, node reaps and backplane faults.
- Error codes: `websocket_envelope_unencodable`, `websocket_frame_length_not_minimal`, `websocket_close_reason_without_code`, `websocket_close_code_not_sendable`, `websocket_permessage_deflate_level_invalid`, `websocket_backplane_mesh_secret_required`, `websocket_backplane_multiple`, `websocket_module_node_id_immutable`, `websocket_module_server_class_invalid`, `websocket_module_handler_class_invalid`, `websocket_service_not_upgrade`, `websocket_service_version_unsupported`, `websocket_service_sapi_unsupported`.
- `CHANGELOG.md`.

### Changed
- Time is read through PRADO's clock seam (`TApplicationClockAwareTrait`); tests inject `TMockClock`. `TMeshBackplane::now()` is removed.
- `THttp2WebSocketProtocol::shutdown()` closes the HTTP/2 session after raising `onClose` for its live connections, freeing the nghttp2 session at once; a later write on one of its connections fails with `websocket_write_failed`.
- `TMeshBackplane::open()` requires a `Secret` (`websocket_backplane_mesh_secret_required`). The mesh challenge is bound to the answering node's id, and a reflected nonce is refused.
- `TFileBackplane` requires an owner-only spool directory; one readable by group or others is refused.
- `TWebSocketModule::setNodeId()` cannot change the id once the cluster exists, and a second `<backplane>` element is refused.
- `TMeshBackplane::SEEN_CAP` raised from 4096 to 65536.
- The development dependency on `belisoful/prado-http2` is `^1.1`; CI covers PHP 8.1 to 8.5 against PRADO `master`, plus Playwright browser tests and the Autobahn fuzzing suite.

### Fixed
- A client that connected and sent nothing hung the whole server (a blocking `STREAM_PEEK`). The handshake was also read synchronously, one byte per call, inside the select loop. Accepted connections now stay non-blocking: the opening bytes are gathered across reactor ticks, and a handshake deadline drops a silent client.
- Idle reaping did not run on a quiet server; deadlines now bound the reactor wait.
- permessage-deflate: inflate steps are sized to the remaining message budget, so a compression bomb overshoots the limit by at most about one input byte's expansion. A truncated DEFLATE stream fails with 1007. Offers and responses with unknown, duplicated, malformed or out-of-range parameters are declined, and the client side enforces the parameters it offered.
- Frame decoding: non-minimal length encodings and 64-bit length overflow are refused. Reading many frames from one buffer is linear rather than quadratic.
- Handshake: exact version token, 24-character base64 key, `Upgrade` parsed as a token list, case-insensitive `Origin`, URL fragments refused, and repeated header fields combined (RFC 7230 section 3.2.2).
- HTTP/2 (RFC 8441): `:scheme`, `:path`, `:authority` and version 13 are validated, and a refused Extended CONNECT is ended instead of left half-open.
- Session teardown: `onClose`, cluster unregistration and the transport close each run even when an earlier step throws, and `MaxConnections` accounting stays correct.
- The send-buffer limit is checked before a frame is queued, so an oversized send does not allocate the payload twice.
- `TWebSocketFrame::close()` refuses a reason without a code and codes that must not be sent.
- Cluster: the Redis backplane purges a node's stale state on open and resyncs on reconnect; File and Redis reap a crashed node's presence; the mesh drops a dead node's link and re-dials seed peers; binary and non-UTF-8 payloads survive every backplane; one failing client no longer stops delivery to the others.

### Upgrading from 1.0.x
- **`onMessage` event parameter.** Handlers attached to `TWebSocketHandler`'s `onMessage` event receive a `TWebSocketMessage` instead of a string. It converts to its payload when used as a string, so `(string) $param` and string concatenation keep working; code that type-checks for `string` must change. Read the opcode with `$param->getOpcode()` or `getIsBinary()`. `IWebSocketHandler::onMessage($connection, $message, $opcode)` is unchanged.
- **`TWebSocketConnection::getLastOpcode()` is removed.** Use `receiveMessage()`, which returns the payload and opcode together.
- **`TWebSocketModule` now extends `TSocketServerModule`.** It owns and configures the server. An application that built its own `TWebSocketServer` can keep doing so and join the module's cluster with `prepareServer()`, or move the server settings onto the module and run `prado-cli websocket/serve`.
- **Mesh clusters need a `Secret`.** `TMeshBackplane` no longer opens without one.
- **File backplane directories must be owner-only** (for example mode `0700`).
- **Stricter protocol validation.** Peers that send non-minimal frame lengths, malformed permessage-deflate parameters, or a non-token `Upgrade` header are now refused.
- **Messages larger than 16 MiB.** A single outbound frame larger than `MaxSendBufferBytes` (default 16 MiB) is refused. When raising `MaxMessageSize` above that, raise `MaxSendBufferBytes` too, or set it to 0 for unlimited.
- **Subclasses.** Several protected `TWebSocketServer` hooks changed signature or were replaced by the reactor-driven flow (`acceptHttp1Session()`, `acceptHttp2Session()`, `dispatchStream()`, `isHttp2Preface()`), and `serve()` takes an optional per-tick callback. `THttp2WebSocketProtocol::rejectStream()` takes extra headers.

### Known limitations
- HTTP/2 streams count toward `MaxConnections` when their session is accepted; a new stream on an existing session past the cap is not refused.
- `TWebSocketConnection::feedMessages()` discards the already-decoded messages of a read when a later frame in that read is malformed.
- Redis commands are blocking round trips (phpredis has no async API); pipelining bounds them to one per envelope.
- The mesh challenge does not prevent a relay through a third node that holds the secret; use a `tls://` transport on untrusted networks.

## [1.0.1] - 2026-08-28

### Changed
- The repository and CI workflow are renamed from `prado-websockets` to `prado-websocket`, matching the package name.
- README installation instructions corrected.

## [1.0.0] - 2026-07-17

### Added
- Initial release: RFC 6455 WebSockets over HTTP/1.1, RFC 8441 over HTTP/2 through `belisoful/prado-http2`, RFC 7692 permessage-deflate, a standalone `TWebSocketServer` that selects HTTP/1.1 or HTTP/2 per connection, a client connection, the `websocket` PRADO service, and a clustering layer (`TWebSocketModule`, `TWebSocketCluster`) with Null, File, Redis and Mesh backplanes.

[1.1.0]: https://github.com/belisoful/prado-websocket/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/belisoful/prado-websocket/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/belisoful/prado-websocket/releases/tag/v1.0.0
