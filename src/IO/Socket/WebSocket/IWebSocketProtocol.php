<?php

/**
 * IWebSocketProtocol interface file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\IO\Socket\TSocketStream;
use Psr\Http\Message\StreamInterface;

/**
 * IWebSocketProtocol interface.
 *
 * A protocol stack turns one accepted transport connection into the logical WebSocket
 * streams it carries.  This is the seam {@see TWebSocketServer} dispatches through, so the
 * server stays indifferent to how many WebSockets share a connection and how they are framed.
 *
 * The transport (a {@see TSocketStream} from {@see \Prado\IO\Socket\TSocketServer}) is a single
 * TCP connection.  A stack maps it to logical streams:
 *
 *  - HTTP/1.1 (RFC 6455, {@see THttp1WebSocketProtocol}): one logical stream per connection.
 *    The opening handshake is the HTTP Upgrade, after which the connection itself carries the
 *    frames.
 *  - HTTP/2 (RFC 8441, {@see THttp2WebSocketProtocol}): many logical streams per connection,
 *    each bootstrapped by an Extended CONNECT on its own HTTP/2 stream.
 *  - HTTP/3 (RFC 9220): the HTTP/2 model over QUIC.  QUIC runs on UDP and needs TLS key hooks
 *    PHP does not expose, so an H3 stack needs a native QUIC backend and a separate UDP
 *    transport, not this TCP server.
 *
 * Every stack negotiates the subprotocol and extensions during its handshake and reports the
 * outcome to the caller with each stream, so the accepted connection can be configured
 * ({@see TWebSocketConnection::setSubprotocol()}, {@see TWebSocketConnection::setExtensions()}).
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc8441.html
 */
interface IWebSocketProtocol
{
	/**
	 * Bootstraps WebSocket logical streams over a transport connection.
	 *
	 * Performs the protocol-specific opening handshake and invokes $onStream once for each
	 * WebSocket-ready logical stream the connection yields.  A single-stream protocol calls it
	 * once; a multiplexing protocol calls it as each stream opens.  $onStream receives the
	 * logical {@see StreamInterface} that carries RFC 6455 frames for that WebSocket and the
	 * accepted handshake as {@see TWebSocketHandshake::acceptConnection()} returns it: the
	 * request `headers` (lower-cased names), the request `target`, the negotiated `subprotocol`
	 * (null for none), and the agreed `extensions` ({@see IWebSocketExtension} instances).
	 *
	 * @param TSocketStream $connection The accepted transport connection.
	 * @param callable(StreamInterface, array{headers: array<string, string>, target: ?string, subprotocol: ?string, extensions: IWebSocketExtension[]}): void $onStream
	 *   Invoked per WebSocket-ready logical stream with its accepted handshake.
	 * @throws TWebSocketException When the handshake fails.
	 */
	public function serve(TSocketStream $connection, callable $onStream): void;
}
