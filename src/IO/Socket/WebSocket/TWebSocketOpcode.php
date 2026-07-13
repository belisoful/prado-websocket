<?php

/**
 * TWebSocketOpcode class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\TEnumerable;

/**
 * TWebSocketOpcode class.
 *
 * Enumerates the RFC 6455 frame opcodes (the low four bits of a frame's first byte).
 *
 * | Constant      | Value | Kind        |
 * |---------------|-------|-------------|
 * | Continuation  | 0x0   | data        |
 * | Text          | 0x1   | data        |
 * | Binary        | 0x2   | data        |
 * | Close         | 0x8   | control     |
 * | Ping          | 0x9   | control     |
 * | Pong          | 0xA   | control     |
 *
 * Opcodes 0x8 and above are control frames, which carry at most 125 payload bytes and are
 * never fragmented.  The frame-header flag bits (FIN, RSV, MASK) and length encoding live in
 * {@see TWebSocketFrameCodec}.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc6455.html#section-5.2
 */
class TWebSocketOpcode extends TEnumerable
{
	public const Continuation = 0x0;
	public const Text = 0x1;
	public const Binary = 0x2;
	public const Close = 0x8;
	public const Ping = 0x9;
	public const Pong = 0xA;

	/**
	 * Indicates whether an opcode is a control frame (Close, Ping, Pong — 0x8 and above).
	 * @param int $opcode The opcode.
	 * @return bool Whether the opcode is a control frame.
	 */
	public static function isControl(int $opcode): bool
	{
		return ($opcode & 0x8) !== 0;
	}

	/**
	 * Indicates whether an opcode is one RFC 6455 defines.  The reserved data opcodes (0x3-0x7)
	 * and reserved control opcodes (0xB-0xF) are undefined, and a frame using one is a protocol
	 * error absent a negotiated extension.
	 * @param int $opcode The opcode.
	 * @return bool Whether the opcode is a defined opcode.
	 */
	public static function isDefined(int $opcode): bool
	{
		return in_array($opcode, [self::Continuation, self::Text, self::Binary, self::Close, self::Ping, self::Pong], true);
	}
}
