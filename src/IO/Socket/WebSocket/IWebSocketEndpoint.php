<?php

/**
 * IWebSocketEndpoint interface file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\IO\Socket\TSocketStream;

/**
 * IWebSocketEndpoint interface.
 *
 * A privileged internal upgrade target a {@see TWebSocketServer} dispatches to by request path,
 * ahead of normal client handling, with its own authentication.  The mesh `/cluster` peer link is
 * one ({@see \Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane}); an `/admin` control channel is
 * another.  The server matches an upgrade against its {@see TWebSocketServer::getEndpoints()
 * endpoints} in order: the first whose {@see matchesTarget()} claims the path authenticates the
 * request, and on success the server completes the handshake and hands the connection to
 * {@see accept()}.  An internal endpoint does not negotiate a subprotocol or extensions.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
interface IWebSocketEndpoint
{
	/**
	 * Indicates whether this endpoint handles an upgrade for the given request target.
	 * @param ?string $target The request target (path).
	 * @return bool Whether this endpoint claims the target.
	 */
	public function matchesTarget(?string $target): bool;

	/**
	 * Authorizes an upgrade request, before the server completes the handshake.  A rejected request
	 * is refused with a 403 and not upgraded.
	 * @param array<string, string> $headers The request headers, keyed lower-case.
	 * @return bool Whether the request is authorized.
	 */
	public function authenticate(array $headers): bool;

	/**
	 * Takes over an authenticated, upgraded connection.  The server has written the 101 response and
	 * set the transport non-blocking; the endpoint owns the connection from here.
	 * @param TWebSocketConnection $connection The upgraded connection.
	 * @param TSocketStream $transport The connection transport.
	 * @param array<string, mixed> $request The parsed upgrade request.
	 */
	public function accept(TWebSocketConnection $connection, TSocketStream $transport, array $request): void;
}
