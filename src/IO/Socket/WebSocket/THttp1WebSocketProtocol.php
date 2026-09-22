<?php

/**
 * THttp1WebSocketProtocol class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\IO\Socket\TSocketStream;
use Psr\Http\Message\StreamInterface;

/**
 * THttp1WebSocketProtocol class.
 *
 * The RFC 6455 protocol stack: one WebSocket per connection, bootstrapped by the HTTP/1.1
 * Upgrade handshake.  {@see serve()} reads and validates the upgrade request, negotiates the
 * subprotocol and extensions, writes the 101 response
 * ({@see TWebSocketHandshake::acceptConnection()}), then yields the connection itself as the
 * single logical stream, since the frames flow over that same socket.
 *
 * The handshake policy is configured through {@see setSubprotocols()}, {@see setExtensions()},
 * {@see setOrigins()}, {@see setAllowedHosts()}, {@see setResponseHeaders()}, and
 * {@see setHandshakeTimeout()}.
 *
 * This is the default stack of {@see TWebSocketServer}.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc6455.html#section-4
 */
class THttp1WebSocketProtocol implements IWebSocketProtocol
{
	/** @var array<string, string> Extra headers added to the 101 response. */
	private array $_responseHeaders = [];

	/** @var string[] The origins allowed to upgrade, empty to allow any. */
	private array $_origins = [];

	/** @var string[] The Host authorities allowed to upgrade, empty to allow any. */
	private array $_allowedHosts = [];

	/** @var string[] The subprotocols supported, in preference order. */
	private array $_subprotocols = [];

	/** @var IWebSocketExtensionNegotiator[] The extension negotiators offered, in preference order. */
	private array $_extensions = [];

	/** @var float The seconds allowed to read the upgrade request, or 0 for no deadline. */
	private float $_handshakeTimeout = 0;

	/**
	 * Returns the extra headers added to the 101 response.
	 * @return array<string, string> The extra response headers.
	 */
	public function getResponseHeaders(): array
	{
		return $this->_responseHeaders;
	}

	/**
	 * Sets extra headers added to the 101 response.
	 * @param array<string, string> $value The extra response headers.
	 */
	public function setResponseHeaders(array $value): void
	{
		$this->_responseHeaders = $value;
	}

	/**
	 * Returns the origins allowed to upgrade.
	 * @return string[] The allowed origins, empty to allow any.
	 */
	public function getOrigins(): array
	{
		return $this->_origins;
	}

	/**
	 * Sets the origins allowed to upgrade; a disallowed origin is refused with a `403`.
	 * @param string[] $value The allowed origins.
	 */
	public function setOrigins(array $value): void
	{
		$this->_origins = array_values($value);
	}

	/**
	 * Returns the Host authorities allowed to upgrade.
	 * @return string[] The allowed hosts, empty to allow any.
	 */
	public function getAllowedHosts(): array
	{
		return $this->_allowedHosts;
	}

	/**
	 * Sets the Host authorities allowed to upgrade; a disallowed Host is refused with a `400`.
	 * @param string[] $value The allowed hosts.
	 */
	public function setAllowedHosts(array $value): void
	{
		$this->_allowedHosts = array_values($value);
	}

	/**
	 * Returns the subprotocols supported.
	 * @return string[] The supported subprotocols, in preference order.
	 */
	public function getSubprotocols(): array
	{
		return $this->_subprotocols;
	}

	/**
	 * Sets the subprotocols supported; the first one the client offers is selected and echoed in
	 * `Sec-WebSocket-Protocol`.
	 * @param string[] $value The supported subprotocols, in preference order.
	 */
	public function setSubprotocols(array $value): void
	{
		$this->_subprotocols = array_values(array_filter(array_map('trim', $value), fn ($p) => $p !== ''));
	}

	/**
	 * Returns the extension negotiators offered during the handshake.
	 * @return IWebSocketExtensionNegotiator[] The negotiators, in preference order.
	 */
	public function getExtensions(): array
	{
		return $this->_extensions;
	}

	/**
	 * Sets the extension negotiators offered during the handshake, in preference order.
	 * @param IWebSocketExtensionNegotiator[] $value The extension negotiators.
	 * @throws TWebSocketException When a value does not implement {@see IWebSocketExtensionNegotiator}.
	 */
	public function setExtensions(array $value): void
	{
		foreach ($value as $negotiator) {
			if (!$negotiator instanceof IWebSocketExtensionNegotiator) {
				throw new TWebSocketException('websocket_extension_negotiator_invalid');
			}
		}
		$this->_extensions = array_values($value);
	}

	/**
	 * Returns the seconds allowed to read the upgrade request.
	 * @return float The handshake deadline in seconds, or 0 for none.
	 */
	public function getHandshakeTimeout(): float
	{
		return $this->_handshakeTimeout;
	}

	/**
	 * Sets the seconds allowed to read the upgrade request; a peer that has not completed the head by
	 * then fails the handshake.
	 * @param float $value The handshake deadline in seconds, or 0 for none.
	 */
	public function setHandshakeTimeout(float $value): void
	{
		$this->_handshakeTimeout = max(0.0, $value);
	}

	/**
	 * Performs the HTTP/1.1 upgrade handshake, enforcing the origin and host allowlists, negotiating
	 * the subprotocol and extensions, and emitting the configured response headers, then yields the
	 * connection as the logical stream with the accepted handshake.
	 * @param TSocketStream $connection The accepted transport connection.
	 * @param callable(StreamInterface, array{headers: array<string, string>, target: ?string, subprotocol: ?string, extensions: IWebSocketExtension[]}): void $onStream
	 *   Invoked with the upgraded connection and the accepted handshake.
	 * @throws TWebSocketException When the request is not a valid WebSocket upgrade, is not read in time, or is disallowed.
	 */
	public function serve(TSocketStream $connection, callable $onStream): void
	{
		$handshake = TWebSocketHandshake::acceptConnection($connection, [
			'subprotocols' => $this->_subprotocols,
			'extensions' => $this->_extensions,
			'origins' => $this->_origins,
			'allowedHosts' => $this->_allowedHosts,
			'headers' => $this->_responseHeaders,
			'timeout' => $this->_handshakeTimeout > 0 ? $this->_handshakeTimeout : null,
		]);
		$onStream($connection, $handshake);
	}
}
