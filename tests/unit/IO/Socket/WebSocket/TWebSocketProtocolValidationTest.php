<?php

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;

/**
 * Covers the RFC 6455 receive-side validations: masking, reserved bits, opcodes, fragmentation,
 * UTF-8, close-frame payloads, and the message-size limit.
 */
class TWebSocketProtocolValidationTest extends PHPUnit\Framework\TestCase
{
	/** @return array{0: TWebSocketConnection, 1: TSocketStream, 2: TSocketStream} A server connection and the two pair ends. */
	private function server(): array
	{
		[$a, $b] = TSocketStream::pair();
		return [new TWebSocketConnection($b, false), $a, $b];
	}

	/** Encodes a frame as a client would send it (masked). */
	private function masked(TWebSocketFrame $frame): string
	{
		return TWebSocketFrameCodec::encode($frame, random_bytes(4));
	}

	/**
	 * Feeds bytes to a server connection and asserts it fails with the expected close code.
	 * @param string $bytes The wire bytes to feed.
	 * @param int $code The expected {@see TWebSocketCloseCode}.
	 * @param string $message The assertion message.
	 */
	private function assertFails(string $bytes, int $code, string $message): void
	{
		[$server, $a, $b] = $this->server();
		try {
			$server->feed($bytes);
			self::fail($message);
		} catch (TWebSocketException $e) {
			self::assertSame($code, $e->getCloseCode(), $message);
		} finally {
			$a->close();
			$b->close();
		}
	}

	public function testServerRejectsUnmaskedClientFrame()
	{
		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::text('hi'));   // no mask key
		$this->assertFails($wire, TWebSocketCloseCode::ProtocolError, 'An unmasked client frame is a protocol error.');
	}

	public function testOversizedFrameIsRejectedFromHeaderBeforeBuffering()
	{
		[$server, $a, $b] = $this->server();
		$server->setMaxMessageSize(1024);
		// A masked binary frame header declaring 100000 bytes; only the header + mask key are fed, no payload.
		$header = "\x82\xFF" . pack('J', 100000) . "\x00\x00\x00\x00";
		try {
			$server->feed($header);
			self::fail('An oversized declared frame is rejected before its payload is buffered.');
		} catch (TWebSocketException $e) {
			self::assertSame(TWebSocketCloseCode::MessageTooBig, $e->getCloseCode(), 'An oversized frame fails with 1009.');
		}
		$a->close();
		$b->close();
	}

	public function testDefaultMaxMessageSizeIsBounded()
	{
		[$server, $a, $b] = $this->server();
		self::assertSame(TWebSocketConnection::DEFAULT_MAX_MESSAGE_SIZE, $server->getMaxMessageSize(), 'A connection bounds message size by default.');
		self::assertGreaterThan(0, TWebSocketConnection::DEFAULT_MAX_MESSAGE_SIZE);
		$a->close();
		$b->close();
	}

	public function testClientRejectsMaskedServerFrame()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::text('hi'), random_bytes(4));   // a masked frame
		try {
			$client->feed($wire);
			self::fail('A masked server frame is a protocol error for a client.');
		} catch (TWebSocketException $e) {
			self::assertSame(TWebSocketCloseCode::ProtocolError, $e->getCloseCode());
		}
		$a->close();
		$b->close();
	}

	public function testReservedBitIsRejected()
	{
		$frame = new TWebSocketFrame(TWebSocketOpcode::Text, 'x', true, true);   // RSV1 set
		$this->assertFails($this->masked($frame), TWebSocketCloseCode::ProtocolError, 'A reserved bit without an extension is a protocol error.');
	}

	public function testUndefinedDataOpcodeIsRejected()
	{
		$frame = new TWebSocketFrame(0x3, 'x');   // reserved data opcode
		$this->assertFails($this->masked($frame), TWebSocketCloseCode::ProtocolError, 'A reserved data opcode is a protocol error.');
	}

	public function testUndefinedControlOpcodeIsRejected()
	{
		$frame = new TWebSocketFrame(0xB, 'x');   // reserved control opcode
		$this->assertFails($this->masked($frame), TWebSocketCloseCode::ProtocolError, 'A reserved control opcode is a protocol error.');
	}

	public function testContinuationWithoutStartIsRejected()
	{
		$frame = TWebSocketFrame::continuation('x', true);
		$this->assertFails($this->masked($frame), TWebSocketCloseCode::ProtocolError, 'A continuation with no message to continue is a protocol error.');
	}

	public function testNewDataFrameDuringFragmentationIsRejected()
	{
		$wire = $this->masked(TWebSocketFrame::text('a', false)) . $this->masked(TWebSocketFrame::text('b', false));
		$this->assertFails($wire, TWebSocketCloseCode::ProtocolError, 'A new data frame during fragmentation is a protocol error.');
	}

	public function testInvalidUtf8TextIsRejected()
	{
		$frame = TWebSocketFrame::text("\xFF\xFE");
		$this->assertFails($this->masked($frame), TWebSocketCloseCode::InvalidFramePayload, 'Invalid UTF-8 in a Text message fails with 1007.');
	}

	public function testValidMultibyteUtf8RoundTrips()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$server = new TWebSocketConnection($b, false);
		$client->send('héllo — 世界');
		self::assertSame('héllo — 世界', $server->receive(), 'Valid multibyte UTF-8 is accepted.');
		$a->close();
		$b->close();
	}

	public function testInvalidUtf8AcrossFragmentsIsRejected()
	{
		// A 2-byte sequence split across fragments whose combined bytes are invalid UTF-8.
		$wire = $this->masked(TWebSocketFrame::text("\xC3", false)) . $this->masked(TWebSocketFrame::continuation("\x28", true));
		$this->assertFails($wire, TWebSocketCloseCode::InvalidFramePayload, 'Invalid UTF-8 spanning fragments fails with 1007.');
	}

	public function testInvalidIncomingCloseCodeIsRejected()
	{
		$frame = TWebSocketFrame::close(TWebSocketCloseCode::NoStatusReceived);   // 1005, status-only
		$this->assertFails($this->masked($frame), TWebSocketCloseCode::ProtocolError, 'A status-only close code is a protocol error when received.');
	}

	public function testOneByteCloseFrameIsRejected()
	{
		$frame = new TWebSocketFrame(TWebSocketOpcode::Close, "\x03");   // one byte, no full code
		$this->assertFails($this->masked($frame), TWebSocketCloseCode::ProtocolError, 'A one-byte Close payload is a protocol error.');
	}

	public function testCloseReasonInvalidUtf8IsRejected()
	{
		$frame = new TWebSocketFrame(TWebSocketOpcode::Close, pack('n', TWebSocketCloseCode::Normal) . "\xFF");
		$this->assertFails($this->masked($frame), TWebSocketCloseCode::InvalidFramePayload, 'A non-UTF-8 Close reason fails with 1007.');
	}

	public function testEmptyCloseFrameIsAccepted()
	{
		[$server, $a, $b] = $this->server();
		$server->feed($this->masked(TWebSocketFrame::close()));   // empty Close, no payload
		self::assertTrue($server->getIsClosed(), 'An empty Close is valid and closes the connection.');
		$a->close();
		$b->close();
	}

	public function testMessageSizeLimitIsEnforced()
	{
		[$server, $a, $b] = $this->server();
		$server->setMaxMessageSize(3);
		try {
			$server->feed($this->masked(TWebSocketFrame::text('abcd')));
			self::fail('A message over the size limit fails.');
		} catch (TWebSocketException $e) {
			self::assertSame(TWebSocketCloseCode::MessageTooBig, $e->getCloseCode());
		}
		$a->close();
		$b->close();
	}

	public function testDisablingMaskValidationAcceptsUnmaskedFrames()
	{
		[$server, $a, $b] = $this->server();
		$server->setValidateMasking(false);
		$messages = $server->feed(TWebSocketFrameCodec::encode(TWebSocketFrame::text('hi')));   // unmasked
		self::assertSame(['hi'], $messages, 'With mask validation disabled, an unmasked frame is accepted.');
		$a->close();
		$b->close();
	}

	public function testDataSendIsSuppressedAfterClose()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$client->close(TWebSocketCloseCode::Normal);
		self::assertSame(0, $client->send('late'), 'A Text send after Close is suppressed.');
		self::assertSame(0, $client->sendBinary('late'), 'A Binary send after Close is suppressed.');
		$a->close();
		$b->close();
	}

	public function testOpcodeIsDefined()
	{
		self::assertTrue(TWebSocketOpcode::isDefined(TWebSocketOpcode::Text));
		self::assertTrue(TWebSocketOpcode::isDefined(TWebSocketOpcode::Pong));
		self::assertFalse(TWebSocketOpcode::isDefined(0x3), 'A reserved data opcode is undefined.');
		self::assertFalse(TWebSocketOpcode::isDefined(0xB), 'A reserved control opcode is undefined.');
	}

	public function testIsValidIncomingCloseCode()
	{
		self::assertTrue(TWebSocketCloseCode::isValidIncoming(TWebSocketCloseCode::Normal));
		self::assertTrue(TWebSocketCloseCode::isValidIncoming(3000));
		self::assertTrue(TWebSocketCloseCode::isValidIncoming(4999));
		self::assertFalse(TWebSocketCloseCode::isValidIncoming(TWebSocketCloseCode::NoStatusReceived), '1005 is status-only.');
		self::assertFalse(TWebSocketCloseCode::isValidIncoming(1004), '1004 is unassigned.');
		self::assertFalse(TWebSocketCloseCode::isValidIncoming(999), 'Codes below 1000 are invalid.');
		self::assertFalse(TWebSocketCloseCode::isValidIncoming(2999), 'The 1012-2999 range is unassigned.');
	}
}
