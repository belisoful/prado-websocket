<?php

/**
 * TWebSocketException class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\Exceptions\TIOException;

/**
 * TWebSocketException class.
 *
 * Reports a WebSocket protocol or handshake failure (a malformed frame, an oversized or
 * fragmented control frame, a rejected or truncated handshake).  It extends
 * {@see TIOException}, so existing IO error handling still catches it.
 *
 * The associated {@see getCloseCode() CloseCode} (default {@see TWebSocketCloseCode::ProtocolError})
 * lets a connection map the failure onto the close code it sends in the Close frame.
 *
 * A failure in {@see TWebSocketConnection::feedMessages()} carries the
 * {@see getDecodedMessages() messages} the same read completed before the malformed frame, so a
 * caller delivers them before it fails the connection.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class TWebSocketException extends TIOException
{
	/** @var int The close code that describes this failure. */
	private int $_closeCode = TWebSocketCloseCode::ProtocolError;

	/** @var TWebSocketMessage[] The complete messages decoded before the failure. */
	private array $_decodedMessages = [];

	/**
	 * Returns the close code that describes this failure.
	 * @return int A {@see TWebSocketCloseCode} value.
	 */
	public function getCloseCode(): int
	{
		return $this->_closeCode;
	}

	/**
	 * Sets the close code that describes this failure.
	 * @param int $value A {@see TWebSocketCloseCode} value.
	 * @return static This instance, for chaining at the throw site.
	 */
	public function setCloseCode(int $value): static
	{
		$this->_closeCode = $value;
		return $this;
	}

	/**
	 * Returns the complete messages decoded before the failure, in order.
	 * {@see TWebSocketConnection::feedMessages()} sets them when a malformed frame follows complete
	 * messages in the same read; elsewhere the list is empty.
	 * @return TWebSocketMessage[] The messages that precede the failure.
	 * @since 1.2.0
	 */
	public function getDecodedMessages(): array
	{
		return $this->_decodedMessages;
	}

	/**
	 * Sets the complete messages decoded before the failure.
	 * @param TWebSocketMessage[] $value The messages that precede the failure, in order.
	 * @return static This instance, for chaining at the throw site.
	 * @since 1.2.0
	 */
	public function setDecodedMessages(array $value): static
	{
		$this->_decodedMessages = array_values($value);
		return $this;
	}
}
