<?php

/**
 * IWebSocketEndpointLimits interface file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

/**
 * IWebSocketEndpointLimits interface.
 *
 * An {@see IWebSocketEndpoint} that declares the limits of the connections it takes over.  The
 * server configures an endpoint connection with its own client limits,
 * {@see TWebSocketServer::getMaxMessageSize() MaxMessageSize} and
 * {@see TWebSocketServer::getMaxSendBufferBytes() MaxSendBufferBytes}, unless the endpoint
 * implements this interface and declares a limit of its own.  An endpoint whose traffic differs
 * from a client's declares them: a mesh peer link carries cluster envelopes, which wrap a client
 * message in base64 and grow past the client limit, and it needs the limit its dialing side uses.
 *
 * Each method returns the limit in bytes, 0 for unlimited, or null to keep the server's.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.2.0
 */
interface IWebSocketEndpointLimits extends IWebSocketEndpoint
{
	/**
	 * Returns the maximum message size applied to a connection this endpoint accepts
	 * ({@see TWebSocketConnection::setMaxMessageSize()}).
	 * @return ?int The maximum size in bytes, 0 for unlimited, or null for the server's MaxMessageSize.
	 */
	public function getEndpointMaxMessageSize(): ?int;

	/**
	 * Returns the maximum queued outbound bytes applied to a connection this endpoint accepts
	 * ({@see TWebSocketConnection::setMaxSendBufferBytes()}).
	 * @return ?int The maximum queued bytes, 0 for unlimited, or null for the server's MaxSendBufferBytes.
	 */
	public function getEndpointMaxSendBufferBytes(): ?int;
}
