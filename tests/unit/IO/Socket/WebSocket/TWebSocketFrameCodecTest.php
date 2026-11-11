<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TInvalidDataValueException;
use Prado\Exceptions\TIOException;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\TStream;

class TWebSocketFrameCodecTest extends TestCase
{
	private function roundTrip(TWebSocketFrame $frame, ?string $maskKey = null): TWebSocketFrame
	{
		$bytes = TWebSocketFrameCodec::encode($frame, $maskKey);
		$decoded = TWebSocketFrameCodec::decode(TStream::fromString($bytes));
		self::assertNotNull($decoded);
		return $decoded;
	}

	public function testDecodeRejectsDeclaredLengthOverTheMaximum()
	{
		// An unmasked binary frame declaring a 70000-byte payload (127 extended length), no payload bytes.
		$header = "\x82\x7F" . pack('J', 70000);
		try {
			TWebSocketFrameCodec::decode(TStream::fromString($header), null, 65536);
			self::fail('An oversized declared length is rejected.');
		} catch (TWebSocketException $e) {
			self::assertSame(TWebSocketCloseCode::MessageTooBig, $e->getCloseCode());
		}
	}

	public function testTryDecodeRejectsOversizedFrameFromHeaderAloneBeforeBuffering()
	{
		// A masked binary frame header declaring 100000 bytes, but only the 10-byte length header present.
		$header = "\x82\xFF" . pack('J', 100000);
		self::assertSame(10, strlen($header), 'only the frame header, no payload or mask key, is buffered');
		try {
			TWebSocketFrameCodec::tryDecode($header, true, 65536);
			self::fail('An oversized declared length is rejected from the header before the payload arrives.');
		} catch (TWebSocketException $e) {
			self::assertSame(TWebSocketCloseCode::MessageTooBig, $e->getCloseCode());
		}
	}

	public function testZeroMaximumIsUnlimited()
	{
		$decoded = TWebSocketFrameCodec::tryDecode(TWebSocketFrameCodec::encode(TWebSocketFrame::binary(str_repeat('Z', 200))), null, 0);
		self::assertNotNull($decoded);
		self::assertSame(200, strlen($decoded['frame']->getPayload()), 'A zero maximum imposes no cap.');
	}

	public function testTextFrameRoundTrip()
	{
		$r = $this->roundTrip(TWebSocketFrame::text('hello'));
		self::assertSame(TWebSocketOpcode::Text, $r->getOpcode());
		self::assertTrue($r->getFin());
		self::assertSame('hello', $r->getPayload());
		self::assertFalse($r->getIsControl());
	}

	public function testMaskedClientFrameRoundTrip()
	{
		$key = "\x12\x34\x56\x78";
		$bytes = TWebSocketFrameCodec::encode(TWebSocketFrame::binary('payload'), $key);
		self::assertSame(TWebSocketFrameCodec::MASK, ord($bytes[1]) & TWebSocketFrameCodec::MASK, 'The MASK bit is set.');
		self::assertStringNotContainsString('payload', $bytes, 'The payload is masked on the wire.');
		self::assertSame('payload', TWebSocketFrameCodec::decode(TStream::fromString($bytes))->getPayload());
	}

	public function testExtended16BitLength()
	{
		$data = str_repeat('A', 200);                 // > 125, <= 0xFFFF -> 126 marker
		$bytes = TWebSocketFrameCodec::encode(TWebSocketFrame::binary($data));
		self::assertSame(126, ord($bytes[1]) & TWebSocketFrameCodec::LENGTH_MASK);
		self::assertSame($data, $this->roundTrip(TWebSocketFrame::binary($data))->getPayload());
	}

	public function testExtended64BitLength()
	{
		$data = str_repeat('Z', 70000);               // > 0xFFFF -> 127 marker
		$bytes = TWebSocketFrameCodec::encode(TWebSocketFrame::binary($data));
		self::assertSame(127, ord($bytes[1]) & TWebSocketFrameCodec::LENGTH_MASK);
		self::assertSame($data, $this->roundTrip(TWebSocketFrame::binary($data))->getPayload());
	}

	public function testCloseFrameCarriesCodeAndReason()
	{
		$r = $this->roundTrip(TWebSocketFrame::close(TWebSocketCloseCode::GoingAway, 'bye'));
		self::assertSame(TWebSocketOpcode::Close, $r->getOpcode());
		self::assertTrue($r->getIsControl());
		self::assertSame(TWebSocketCloseCode::GoingAway, $r->getCloseCode());
		self::assertSame('bye', $r->getCloseReason());
	}

	public function testRsvBitsDoNotCorruptTheOpcode()
	{
		// Regression: the opcode is the low 4 bits (mask 0x0F), not 0x7F.
		$bytes = TWebSocketFrameCodec::encode(new TWebSocketFrame(TWebSocketOpcode::Text, 'x', true, true, false, true));
		$r = TWebSocketFrameCodec::decode(TStream::fromString($bytes));
		self::assertSame(TWebSocketOpcode::Text, $r->getOpcode(), 'RSV bits must not leak into the opcode.');
		self::assertTrue($r->getRsv1());
		self::assertTrue($r->getRsv3());
		self::assertFalse($r->getRsv2());
	}

	/** Asserts a call throws a TWebSocketException, which is a TIOException, with the given error code. */
	private function assertThrowsCode(string $code, callable $call, string $message): void
	{
		try {
			$call();
			self::fail($message);
		} catch (TWebSocketException $e) {
			self::assertInstanceOf(TIOException::class, $e, 'A protocol failure is caught by IO error handling.');
			self::assertSame($code, $e->getErrorCode(), $message);
		}
	}

	public function testControlFrameTooLongThrows()
	{
		$this->assertThrowsCode(
			'websocket_control_frame_too_long',
			fn () => TWebSocketFrameCodec::encode(TWebSocketFrame::ping(str_repeat('x', 126))),
			'A control frame carries at most 125 payload bytes.',
		);
		self::assertSame(127, strlen(TWebSocketFrameCodec::encode(TWebSocketFrame::ping(str_repeat('x', 125)))), 'A 125-byte control payload is the longest allowed.');
	}

	public function testFragmentedControlFrameThrows()
	{
		foreach ([TWebSocketOpcode::Ping, TWebSocketOpcode::Pong, TWebSocketOpcode::Close] as $opcode) {
			$this->assertThrowsCode(
				'websocket_control_frame_fragmented',
				fn () => TWebSocketFrameCodec::encode(new TWebSocketFrame($opcode, 'x', false)),
				"Control opcode $opcode is not encoded without FIN.",
			);
		}
	}

	public function testDecodeRejectsAFragmentedControlFrame()
	{
		// A control frame with FIN clear (RFC 6455 §5.5): Ping 0x09, Pong 0x0A, Close 0x08, each unmasked and masked.
		foreach (["\x09\x01x", "\x0a\x00", "\x08\x02\x03\xe8", "\x09\x81\x01\x02\x03\x04y"] as $wire) {
			$this->assertBothDecodersReject($wire, 'websocket_control_frame_fragmented', 'A control frame without FIN is refused on ' . bin2hex($wire) . '.');
		}
		$this->assertThrowsCode(
			'websocket_control_frame_fragmented',
			fn () => TWebSocketFrameCodec::tryDecode("\x09\x05"),
			'The header alone refuses a fragmented control frame, before its payload arrives.',
		);
		$this->assertThrowsCode(
			'websocket_control_frame_fragmented',
			fn () => TWebSocketFrameCodec::tryDecode("\x09\x85", true),
			'The same holds for a masked header whose key is still arriving.',
		);

		// FIN clear is what a data frame uses to fragment: those decode.
		$fragment = TWebSocketFrameCodec::tryDecode("\x01\x01x");
		self::assertFalse($fragment['frame']->getFin());
		self::assertSame(TWebSocketOpcode::Text, $fragment['frame']->getOpcode());
		self::assertFalse(TWebSocketFrameCodec::tryDecode("\x00\x01y")['frame']->getFin(), 'A continuation frame without FIN decodes.');
		self::assertTrue(TWebSocketFrameCodec::tryDecode("\x89\x01x")['frame']->getFin(), 'A control frame with FIN decodes.');
	}

	public function testDecodeReturnsNullAtCleanEof()
	{
		self::assertNull(TWebSocketFrameCodec::decode(TStream::fromString('')));
	}

	public function testDecodeThrowsOnTruncatedFrame()
	{
		// FIN+Text, length 5, but no payload follows.
		$this->assertThrowsCode(
			'websocket_frame_incomplete',
			fn () => TWebSocketFrameCodec::decode(TStream::fromString("\x81\x05")),
			'A frame cut short by the end of the stream is refused.',
		);
		$this->assertThrowsCode(
			'websocket_frame_incomplete',
			fn () => TWebSocketFrameCodec::decode(TStream::fromString("\x81")),
			'A header cut short by the end of the stream is refused.',
		);
	}

	public function testFragmentedMessageSequence()
	{
		$first = TWebSocketFrameCodec::encode(TWebSocketFrame::text('Hel', false));
		$cont = TWebSocketFrameCodec::encode(TWebSocketFrame::continuation('lo', true));
		$stream = TStream::fromString($first . $cont);
		$a = TWebSocketFrameCodec::decode($stream);
		$b = TWebSocketFrameCodec::decode($stream);
		self::assertSame(TWebSocketOpcode::Text, $a->getOpcode());
		self::assertFalse($a->getFin());
		self::assertSame(TWebSocketOpcode::Continuation, $b->getOpcode());
		self::assertTrue($b->getFin());
		self::assertSame('Hello', $a->getPayload() . $b->getPayload());
	}

	public function testTryDecodeReturnsNullUntilComplete()
	{
		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::text('hello'));
		for ($i = 1; $i < strlen($wire); $i++) {
			self::assertNull(TWebSocketFrameCodec::tryDecode(substr($wire, 0, $i)), "incomplete at $i bytes");
		}
		$decoded = TWebSocketFrameCodec::tryDecode($wire);
		self::assertSame('hello', $decoded['frame']->getPayload());
		self::assertSame(strlen($wire), $decoded['length']);
	}

	public function testTryDecodeMaskedExtendedLengthStopsAtBoundary()
	{
		$key = random_bytes(4);
		$payload = str_repeat('x', 300);                 // forces the 16-bit extended length
		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::binary($payload), $key);
		$decoded = TWebSocketFrameCodec::tryDecode($wire . 'TRAILING');
		self::assertSame($payload, $decoded['frame']->getPayload());
		self::assertSame(strlen($wire), $decoded['length'], 'The reported length excludes trailing bytes.');
	}

	public function testTryDecodeThrowsOnOversizedControl()
	{
		// Close opcode (0x88) with a 126 extended length of 128 exceeds the 125-byte control limit.
		$this->assertBothDecodersReject("\x88\x7e\x00\x80" . str_repeat('x', 128), 'websocket_control_frame_too_long', 'A control frame over 125 bytes is refused.');
		$this->assertThrowsCode(
			'websocket_control_frame_too_long',
			fn () => TWebSocketFrameCodec::tryDecode("\x88\x7e\x00\x80"),
			'The header alone refuses it, before the payload arrives.',
		);
	}

	// ---- Length encoding ------------------------------------------------------

	/** Asserts both decoders refuse the wire bytes with the given error code. */
	private function assertBothDecodersReject(string $wire, string $code, string $message): void
	{
		try {
			TWebSocketFrameCodec::decode(TStream::fromString($wire));
			self::fail("decode(): $message");
		} catch (TWebSocketException $e) {
			self::assertSame($code, $e->getErrorCode(), "decode(): $message");
		}
		try {
			TWebSocketFrameCodec::tryDecode($wire);
			self::fail("tryDecode(): $message");
		} catch (TWebSocketException $e) {
			self::assertSame($code, $e->getErrorCode(), "tryDecode(): $message");
		}
	}

	public function testNonMinimal16BitLengthIsRejected()
	{
		$wire = "\x82\x7E\x00\x64" . str_repeat('x', 100);   // 100 bytes declared with the 126 marker
		$this->assertBothDecodersReject($wire, 'websocket_frame_length_not_minimal', 'A length under 126 must use the 7-bit field (RFC 6455 §5.2).');
		$minimal = "\x82\x7E\x00\x7E" . str_repeat('x', 126);
		self::assertSame(126, strlen(TWebSocketFrameCodec::tryDecode($minimal)['frame']->getPayload()), '126 is the smallest 16-bit length.');
	}

	public function testNonMinimal64BitLengthIsRejected()
	{
		$wire = "\x82\x7F" . pack('J', 1000) . str_repeat('x', 1000);   // 1000 bytes declared with the 127 marker
		$this->assertBothDecodersReject($wire, 'websocket_frame_length_not_minimal', 'A length under 65536 must use the 16-bit field (RFC 6455 §5.2).');
		$wire = "\x82\x7F" . pack('J', 0xFFFF) . str_repeat('x', 0xFFFF);
		$this->assertBothDecodersReject($wire, 'websocket_frame_length_not_minimal', '65535 still fits the 16-bit field.');
		$minimal = "\x82\x7F" . pack('J', 0x10000) . str_repeat('x', 0x10000);
		self::assertSame(0x10000, strlen(TWebSocketFrameCodec::tryDecode($minimal)['frame']->getPayload()), '65536 is the smallest 64-bit length.');
	}

	public function testLengthWithTheMostSignificantBitSetIsRejected()
	{
		$wire = "\x82\x7F\x80\x00\x00\x00\x00\x00\x00\x00";   // RFC 6455 §5.2: the MSB of a 64-bit length must be 0
		$this->assertBothDecodersReject($wire, 'websocket_frame_length_invalid', 'A 64-bit length with the MSB set is invalid.');
	}

	public function testNearMaximumLengthDoesNotOverflowWithAnUnlimitedMaximum()
	{
		$header = "\x82\x7F" . pack('J', PHP_INT_MAX);
		self::assertNull(TWebSocketFrameCodec::tryDecode($header . 'abc', null, 0), 'An incomplete huge frame waits for more bytes without overflowing the offset arithmetic.');
		self::assertNull(TWebSocketFrameCodec::tryDecode($header . 'abc', null, 0), 'Repeated calls are stable.');
		try {
			TWebSocketFrameCodec::decode(TStream::fromString($header . 'abc'), null, 0);
			self::fail('A huge declared length on a short stream is a truncated frame.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_frame_incomplete', $e->getErrorCode(), 'The blocking decoder reads in bounded steps and reports the truncation.');
		}
	}

	// ---- Mask requirement -----------------------------------------------------

	public function testDecodeEnforcesTheMaskRequirement()
	{
		$masked = TWebSocketFrameCodec::encode(TWebSocketFrame::text('hi'), random_bytes(4));
		$unmasked = TWebSocketFrameCodec::encode(TWebSocketFrame::text('hi'));

		self::assertSame('hi', TWebSocketFrameCodec::decode(TStream::fromString($masked), true)->getPayload(), 'A server accepts a masked frame.');
		self::assertSame('hi', TWebSocketFrameCodec::decode(TStream::fromString($unmasked), false)->getPayload(), 'A client accepts an unmasked frame.');
		self::assertSame('hi', TWebSocketFrameCodec::tryDecode($masked, true)['frame']->getPayload());
		self::assertSame('hi', TWebSocketFrameCodec::tryDecode($unmasked, false)['frame']->getPayload());

		$this->assertBothDecodersRejectWithMask($unmasked, true, 'websocket_frame_not_masked', 'A server refuses an unmasked frame.');
		$this->assertBothDecodersRejectWithMask($masked, false, 'websocket_frame_masked', 'A client refuses a masked frame.');
	}

	/** Asserts both decoders refuse the wire bytes under the given mask requirement. */
	private function assertBothDecodersRejectWithMask(string $wire, bool $requireMask, string $code, string $message): void
	{
		try {
			TWebSocketFrameCodec::decode(TStream::fromString($wire), $requireMask);
			self::fail("decode(): $message");
		} catch (TWebSocketException $e) {
			self::assertSame($code, $e->getErrorCode(), "decode(): $message");
			self::assertSame(TWebSocketCloseCode::ProtocolError, $e->getCloseCode());
		}
		try {
			TWebSocketFrameCodec::tryDecode($wire, $requireMask);
			self::fail("tryDecode(): $message");
		} catch (TWebSocketException $e) {
			self::assertSame($code, $e->getErrorCode(), "tryDecode(): $message");
		}
	}

	public function testMaskedEmptyPayloadRoundTrips()
	{
		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::text(''), "\x01\x02\x03\x04");
		self::assertSame(6, strlen($wire), 'Two header bytes and the four-byte key, no payload.');
		self::assertSame(TWebSocketFrameCodec::MASK, ord($wire[1]) & TWebSocketFrameCodec::MASK);
		$decoded = TWebSocketFrameCodec::decode(TStream::fromString($wire), true);
		self::assertSame('', $decoded->getPayload());
		self::assertSame(TWebSocketOpcode::Text, $decoded->getOpcode());
		$buffered = TWebSocketFrameCodec::tryDecode($wire . 'next', true);
		self::assertSame('', $buffered['frame']->getPayload());
		self::assertSame(6, $buffered['length'], 'The consumed length includes the mask key.');
		self::assertNull(TWebSocketFrameCodec::tryDecode(substr($wire, 0, 5), true), 'A masked frame is incomplete until its key arrives.');
	}

	// ---- Close frame factory --------------------------------------------------

	public function testCloseFactoryRefusesAReasonWithoutACode()
	{
		try {
			TWebSocketFrame::close(null, 'bye');
			self::fail('A reason needs a code to precede it.');
		} catch (TInvalidDataValueException $e) {
			self::assertSame('websocket_close_reason_without_code', $e->getErrorCode());
		}
		self::assertSame('', TWebSocketFrame::close()->getPayload(), 'An empty Close has no payload.');
		self::assertNull(TWebSocketFrame::close()->getCloseCode());
	}

	public function testCloseFactoryRefusesAnUnsendableCode()
	{
		foreach ([TWebSocketCloseCode::NoStatusReceived, TWebSocketCloseCode::Abnormal, TWebSocketCloseCode::TLSHandshake, 1004, 1016, 70000, -1, 0] as $code) {
			try {
				TWebSocketFrame::close($code);
				self::fail("Close code $code is not sendable.");
			} catch (TInvalidDataValueException $e) {
				self::assertSame('websocket_close_code_not_sendable', $e->getErrorCode());
			}
		}
	}

	public function testCloseFactoryAcceptsSendableCodes()
	{
		$frame = TWebSocketFrame::close(TWebSocketCloseCode::ServiceRestart, 'restarting');
		self::assertSame(TWebSocketCloseCode::ServiceRestart, $frame->getCloseCode());
		self::assertSame('restarting', $frame->getCloseReason());
		self::assertSame(4999, TWebSocketFrame::close(4999)->getCloseCode());
		self::assertSame('', TWebSocketFrame::close(1000)->getCloseReason());
	}

	public function testCloseFactoryBoundsTheReasonAndRequiresUtf8()
	{
		$longest = str_repeat('é', 61) . 'x';                  // 123 bytes of valid UTF-8
		$frame = TWebSocketFrame::close(TWebSocketCloseCode::Normal, $longest);
		self::assertSame($longest, $frame->getCloseReason(), 'A 123-byte reason fits the 125-byte control payload with its code.');
		self::assertSame(125, strlen(TWebSocketFrameCodec::encode($frame)) - 2);
		try {
			TWebSocketFrame::close(TWebSocketCloseCode::Normal, $longest . 'y');
			self::fail('A 124-byte reason does not fit a control frame.');
		} catch (TInvalidDataValueException $e) {
			self::assertSame('websocket_close_reason_too_long', $e->getErrorCode());
		}
		try {
			TWebSocketFrame::close(TWebSocketCloseCode::Normal, "bad \xc3\x28");
			self::fail('A reason that is not UTF-8 is refused.');
		} catch (TInvalidDataValueException $e) {
			self::assertSame('websocket_close_reason_not_utf8', $e->getErrorCode());
		}
	}
}
