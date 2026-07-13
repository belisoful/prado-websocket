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
 * Upgrade handshake.  {@see serve()} reads and validates the upgrade request, writes the 101
 * response ({@see TWebSocketHandshake::acceptConnection()}), then yields the connection itself
 * as the single logical stream, since the frames flow over that same socket.
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

	/**
	 * Returns the extra headers added to the 101 response.
	 * @return array<string, string> The extra response headers.
	 */
	public function getResponseHeaders(): array
	{
		return $this->_responseHeaders;
	}

	/**
	 * Sets extra headers added to the 101 response (e.g. Sec-WebSocket-Protocol).
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
	 * Performs the HTTP/1.1 upgrade handshake — enforcing the origin and host allowlists and emitting
	 * the configured response headers — and yields the connection as the logical stream.
	 * @param TSocketStream $connection The accepted transport connection.
	 * @param callable(StreamInterface): void $onStream Invoked with the upgraded connection.
	 * @throws TWebSocketException When the request is not a valid WebSocket upgrade or is disallowed.
	 */
	public function serve(TSocketStream $connection, callable $onStream): void
	{
		TWebSocketHandshake::acceptConnection($connection, [
			'headers' => $this->_responseHeaders,
			'origins' => $this->_origins,
			'allowedHosts' => $this->_allowedHosts,
		]);
		$onStream($connection);
	}
}
