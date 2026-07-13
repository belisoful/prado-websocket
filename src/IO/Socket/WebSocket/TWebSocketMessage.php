<?php

/**
 * TWebSocketMessage class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\TComponent;

/**
 * TWebSocketMessage class.
 *
 * A complete application message decoded from a connection: its payload paired with the opcode of
 * its first frame ({@see TWebSocketOpcode::Text} or {@see TWebSocketOpcode::Binary}).
 * {@see TWebSocketConnection::feedMessages()} returns these so a handler echoes each message under
 * its own opcode, even when one read yields several messages of different kinds.  It stringifies to
 * its payload, so it substitutes for the raw message where only the bytes matter.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class TWebSocketMessage extends TComponent implements \Stringable
{
	/** @var int The opcode of the message's first frame. */
	private int $_opcode;

	/** @var string The decoded message payload. */
	private string $_payload;

	/**
	 * @param int $opcode The opcode of the message's first frame (a {@see TWebSocketOpcode} value).
	 * @param string $payload The decoded message payload.
	 */
	public function __construct(int $opcode, string $payload)
	{
		$this->_opcode = $opcode;
		$this->_payload = $payload;
		parent::__construct();
	}

	/**
	 * Returns the opcode of the message's first frame.
	 * @return int A {@see TWebSocketOpcode} value.
	 */
	public function getOpcode(): int
	{
		return $this->_opcode;
	}

	/**
	 * Returns the decoded message payload.
	 * @return string The payload.
	 */
	public function getPayload(): string
	{
		return $this->_payload;
	}

	/**
	 * Returns whether the message is a Text message.
	 * @return bool Whether the opcode is {@see TWebSocketOpcode::Text}.
	 */
	public function getIsText(): bool
	{
		return $this->_opcode === TWebSocketOpcode::Text;
	}

	/**
	 * Returns whether the message is a Binary message.
	 * @return bool Whether the opcode is {@see TWebSocketOpcode::Binary}.
	 */
	public function getIsBinary(): bool
	{
		return $this->_opcode === TWebSocketOpcode::Binary;
	}

	/**
	 * Returns the payload, so the message stands in for its bytes in a string context.
	 * @return string The payload.
	 */
	public function __toString(): string
	{
		return $this->_payload;
	}
}
