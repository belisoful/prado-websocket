<?php

/**
 * TWebSocketFrameCodec class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\Prado;
use Psr\Http\Message\StreamInterface;

/**
 * TWebSocketFrameCodec class.
 *
 * Encodes a {@see TWebSocketFrame} to RFC 6455 wire bytes and decodes one frame from a
 * stream.  The first byte carries FIN, the three RSV bits, and the 4-bit opcode; the second
 * carries the MASK bit and a 7-bit length that selects the extended length (126 -> 16-bit,
 * 127 -> 64-bit).  A masked frame is followed by a 4-byte key, and its payload is XORed with
 * that key (clients mask, servers do not).
 *
 * {@see encode()} masks when a 4-byte key is given.  {@see decode()} reads exactly one frame
 * (blocking on the stream), unmasks when needed, and enforces the control-frame rules (at most
 * 125 payload bytes, never fragmented).  It returns null on a clean end of stream before a
 * frame begins, and throws on a truncated frame.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 4.4.0
 * @see https://www.rfc-editor.org/rfc/rfc6455.html#section-5.2
 */
class TWebSocketFrameCodec
{
	/** @var int The FIN flag in the first byte. */
	public const FIN = 0x80;

	/** @var int The RSV1 flag in the first byte. */
	public const RSV1 = 0x40;

	/** @var int The RSV2 flag in the first byte. */
	public const RSV2 = 0x20;

	/** @var int The RSV3 flag in the first byte. */
	public const RSV3 = 0x10;

	/** @var int The opcode mask in the first byte (low four bits). */
	public const OPCODE_MASK = 0x0F;

	/** @var int The MASK flag in the second byte. */
	public const MASK = 0x80;

	/** @var int The 7-bit length mask in the second byte. */
	public const LENGTH_MASK = 0x7F;

	/** @var int The maximum payload of a control frame. */
	public const MAX_CONTROL_PAYLOAD = 125;

	/**
	 * Encodes a frame to its wire bytes, masking the payload when a key is given.
	 * @param TWebSocketFrame $frame The frame to encode.
	 * @param ?string $maskKey A 4-byte masking key (client side), or null for no mask (server side).
	 * @throws TWebSocketException When a control frame exceeds 125 bytes or is fragmented.
	 * @return string The encoded frame bytes.
	 */
	public static function encode(TWebSocketFrame $frame, ?string $maskKey = null): string
	{
		$opcode = $frame->getOpcode();
		$payload = $frame->getPayload();
		if (TWebSocketOpcode::isControl($opcode)) {
			if (strlen($payload) > self::MAX_CONTROL_PAYLOAD) {
				throw new TWebSocketException('websocket_control_frame_too_long', strlen($payload));
			}
			if (!$frame->getFin()) {
				throw new TWebSocketException('websocket_control_frame_fragmented', $opcode);
			}
		}
		$byte0 = ($frame->getFin() ? self::FIN : 0)
			| ($frame->getRsv1() ? self::RSV1 : 0)
			| ($frame->getRsv2() ? self::RSV2 : 0)
			| ($frame->getRsv3() ? self::RSV3 : 0)
			| ($opcode & self::OPCODE_MASK);

		$length = strlen($payload);
		$maskBit = $maskKey !== null ? self::MASK : 0;
		if ($length <= 125) {
			$header = chr($byte0) . chr($maskBit | $length);
		} elseif ($length <= 0xFFFF) {
			$header = chr($byte0) . chr($maskBit | 126) . pack('n', $length);
		} else {
			$header = chr($byte0) . chr($maskBit | 127) . pack('J', $length);
		}
		if ($maskKey !== null) {
			return $header . $maskKey . self::applyMask($payload, $maskKey);
		}
		return $header . $payload;
	}

	/**
	 * Decodes one frame from a stream.
	 * @param StreamInterface $stream The stream positioned at a frame boundary.
	 * @param ?bool $requireMask The expected mask state: true requires a masked frame (server reading
	 *   a client), false requires an unmasked frame (client reading a server), null skips the check.
	 * @throws TWebSocketException When a frame is truncated, a control frame is malformed, the mask
	 *   state is wrong, or a 64-bit length exceeds the signed integer range.
	 * @return ?TWebSocketFrame The frame, or null at a clean end of stream before any frame byte.
	 */
	public static function decode(StreamInterface $stream, ?bool $requireMask = null): ?TWebSocketFrame
	{
		$head = self::readOrNull($stream, 2);
		if ($head === null) {
			return null;
		}
		$byte0 = ord($head[0]);
		$byte1 = ord($head[1]);
		$fin = ($byte0 & self::FIN) !== 0;
		$opcode = $byte0 & self::OPCODE_MASK;
		$masked = ($byte1 & self::MASK) !== 0;
		self::assertMask($masked, $requireMask);
		$length = $byte1 & self::LENGTH_MASK;
		if ($length === 126) {
			$length = unpack('n', self::readExact($stream, 2))[1];
		} elseif ($length === 127) {
			$length = unpack('J', self::readExact($stream, 8))[1];
			if ($length < 0) {
				throw new TWebSocketException('websocket_frame_length_invalid');
			}
		}
		if (TWebSocketOpcode::isControl($opcode)) {
			if ($length > self::MAX_CONTROL_PAYLOAD) {
				throw new TWebSocketException('websocket_control_frame_too_long', $length);
			}
			if (!$fin) {
				throw new TWebSocketException('websocket_control_frame_fragmented', $opcode);
			}
		}
		$maskKey = $masked ? self::readExact($stream, 4) : null;
		$payload = $length > 0 ? self::readExact($stream, $length) : '';
		if ($maskKey !== null && $payload !== '') {
			$payload = self::applyMask($payload, $maskKey);
		}
		return Prado::createComponent(
			TWebSocketFrame::class,
			$opcode,
			$payload,
			$fin,
			($byte0 & self::RSV1) !== 0,
			($byte0 & self::RSV2) !== 0,
			($byte0 & self::RSV3) !== 0,
		);
	}

	/**
	 * Decodes one frame from the front of an in-memory buffer, without consuming the buffer.
	 *
	 * This is the non-blocking counterpart to {@see decode()}: it returns null when the buffer
	 * does not yet hold a complete frame (the caller reads more bytes and retries), and the
	 * consumed length when it does, so the caller can advance past the frame.  Protocol errors
	 * (a malformed control frame, an out-of-range 64-bit length) throw as in {@see decode()}.
	 *
	 * @param string $buffer The accumulated bytes, positioned at a frame boundary.
	 * @param ?bool $requireMask The expected mask state: true requires a masked frame (server reading
	 *   a client), false requires an unmasked frame (client reading a server), null skips the check.
	 * @throws TWebSocketException When a control frame is malformed, the mask state is wrong, or a
	 *   64-bit length is out of range.
	 * @return ?array{frame: TWebSocketFrame, length: int} The frame and its byte length, or null when incomplete.
	 */
	public static function tryDecode(string $buffer, ?bool $requireMask = null): ?array
	{
		$available = strlen($buffer);
		if ($available < 2) {
			return null;
		}
		$byte0 = ord($buffer[0]);
		$byte1 = ord($buffer[1]);
		$fin = ($byte0 & self::FIN) !== 0;
		$opcode = $byte0 & self::OPCODE_MASK;
		$masked = ($byte1 & self::MASK) !== 0;
		self::assertMask($masked, $requireMask);
		$length = $byte1 & self::LENGTH_MASK;
		$offset = 2;
		if ($length === 126) {
			if ($available < $offset + 2) {
				return null;
			}
			$length = unpack('n', substr($buffer, $offset, 2))[1];
			$offset += 2;
		} elseif ($length === 127) {
			if ($available < $offset + 8) {
				return null;
			}
			$length = unpack('J', substr($buffer, $offset, 8))[1];
			if ($length < 0) {
				throw new TWebSocketException('websocket_frame_length_invalid');
			}
			$offset += 8;
		}
		if (TWebSocketOpcode::isControl($opcode)) {
			if ($length > self::MAX_CONTROL_PAYLOAD) {
				throw new TWebSocketException('websocket_control_frame_too_long', $length);
			}
			if (!$fin) {
				throw new TWebSocketException('websocket_control_frame_fragmented', $opcode);
			}
		}
		$maskKey = null;
		if ($masked) {
			if ($available < $offset + 4) {
				return null;
			}
			$maskKey = substr($buffer, $offset, 4);
			$offset += 4;
		}
		if ($available < $offset + $length) {
			return null;
		}
		$payload = $length > 0 ? substr($buffer, $offset, $length) : '';
		$offset += $length;
		if ($maskKey !== null && $payload !== '') {
			$payload = self::applyMask($payload, $maskKey);
		}
		$frame = Prado::createComponent(
			TWebSocketFrame::class,
			$opcode,
			$payload,
			$fin,
			($byte0 & self::RSV1) !== 0,
			($byte0 & self::RSV2) !== 0,
			($byte0 & self::RSV3) !== 0,
		);
		return ['frame' => $frame, 'length' => $offset];
	}

	/**
	 * Asserts a frame's mask state matches what the reading role requires.  A server MUST receive
	 * masked frames and a client MUST receive unmasked frames (RFC 6455 section 5.1); a mismatch
	 * is a protocol error.
	 * @param bool $masked Whether the frame carried the MASK bit.
	 * @param ?bool $requireMask The required state (true masked, false unmasked), or null to skip.
	 * @throws TWebSocketException When the mask state does not match the requirement.
	 */
	private static function assertMask(bool $masked, ?bool $requireMask): void
	{
		if ($requireMask === null || $masked === $requireMask) {
			return;
		}
		throw new TWebSocketException($requireMask ? 'websocket_frame_not_masked' : 'websocket_frame_masked');
	}

	/**
	 * XORs a payload with a 4-byte masking key (the operation is its own inverse).
	 * @param string $payload The bytes to mask or unmask.
	 * @param string $maskKey The 4-byte key.
	 * @return string The masked (or unmasked) bytes.
	 */
	public static function applyMask(string $payload, string $maskKey): string
	{
		$length = strlen($payload);
		for ($i = 0; $i < $length; $i++) {
			$payload[$i] = $payload[$i] ^ $maskKey[$i & 3];
		}
		return $payload;
	}

	/**
	 * Reads exactly $count bytes, throwing when the stream ends first.
	 * @param StreamInterface $stream The stream to read.
	 * @param int $count The number of bytes required.
	 * @throws TWebSocketException When fewer than $count bytes are available.
	 * @return string The bytes read.
	 */
	private static function readExact(StreamInterface $stream, int $count): string
	{
		$data = '';
		while (strlen($data) < $count) {
			$chunk = $stream->eof() ? '' : $stream->read($count - strlen($data));
			if ($chunk === '') {
				throw new TWebSocketException('websocket_frame_incomplete', $count, strlen($data));
			}
			$data .= $chunk;
		}
		return $data;
	}

	/**
	 * Reads $count bytes, or returns null when the stream is at a clean end before the first byte.
	 * @param StreamInterface $stream The stream to read.
	 * @param int $count The number of bytes required (>= 1).
	 * @return ?string The bytes read, or null at a clean end of stream.
	 */
	private static function readOrNull(StreamInterface $stream, int $count): ?string
	{
		if ($stream->eof()) {
			return null;
		}
		$first = $stream->read(1);
		if ($first === '') {
			return null;
		}
		return $first . ($count > 1 ? self::readExact($stream, $count - 1) : '');
	}
}
