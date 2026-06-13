<?php

use Prado\Exceptions\TIOException;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\TStream;

class TWebSocketFrameCodecTest extends PHPUnit\Framework\TestCase
{
	private function roundTrip(TWebSocketFrame $frame, ?string $maskKey = null): TWebSocketFrame
	{
		$bytes = TWebSocketFrameCodec::encode($frame, $maskKey);
		$decoded = TWebSocketFrameCodec::decode(TStream::fromString($bytes));
		self::assertNotNull($decoded);
		return $decoded;
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

	public function testControlFrameTooLongThrows()
	{
		self::expectException(TIOException::class);
		TWebSocketFrameCodec::encode(TWebSocketFrame::ping(str_repeat('x', 126)));
	}

	public function testFragmentedControlFrameThrows()
	{
		self::expectException(TIOException::class);
		TWebSocketFrameCodec::encode(new TWebSocketFrame(TWebSocketOpcode::Ping, 'x', false));
	}

	public function testDecodeReturnsNullAtCleanEof()
	{
		self::assertNull(TWebSocketFrameCodec::decode(TStream::fromString('')));
	}

	public function testDecodeThrowsOnTruncatedFrame()
	{
		// FIN+Text, length 5, but no payload follows.
		self::expectException(TIOException::class);
		TWebSocketFrameCodec::decode(TStream::fromString("\x81\x05"));
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
		$this->expectException(TIOException::class);
		// Close opcode (0x88) with a 126 extended length of 128 exceeds the 125-byte control limit.
		TWebSocketFrameCodec::tryDecode("\x88\x7e\x00\x80" . str_repeat('x', 128));
	}
}
