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
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 4.4.0
 */
class TWebSocketException extends TIOException
{
	/** @var int The close code that describes this failure. */
	private int $_closeCode = TWebSocketCloseCode::ProtocolError;

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
}
