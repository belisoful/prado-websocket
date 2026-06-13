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
 *  - HTTP/2 (RFC 8441): many logical streams per connection, each bootstrapped by an Extended
 *    CONNECT on its own HTTP/2 stream.  A future stack implements this seam.
 *  - HTTP/3 (RFC 9220): the HTTP/2 model over QUIC.  QUIC runs on UDP and needs TLS key hooks
 *    PHP does not expose, so a future H3 stack needs a native QUIC backend and a separate UDP
 *    transport, not this TCP server.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 4.4.0
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
	 * logical {@see StreamInterface} that carries RFC 6455 frames for that WebSocket.
	 *
	 * @param TSocketStream $connection The accepted transport connection.
	 * @param callable(StreamInterface): void $onStream Invoked per WebSocket-ready logical stream.
	 * @throws TWebSocketException When the handshake fails.
	 */
	public function serve(TSocketStream $connection, callable $onStream): void;
}
