<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\IWebSocketExtension;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;

/** A test extension that XORs the payload with a key and owns a configurable RSV bit. */
class ScrambleExtension implements IWebSocketExtension
{
	public function __construct(private int $key = 0xAA, private int $rsv = IWebSocketExtension::RSV1)
	{
	}

	public function getName(): string
	{
		return 'x-scramble';
	}

	public function getReservedRsv(): int
	{
		return $this->rsv;
	}

	public function encodeMessage(string $payload): array
	{
		return [$this->scramble($payload), $this->rsv];
	}

	public function decodeMessage(string $payload, int $rsv): string
	{
		return ($rsv & $this->rsv) !== 0 ? $this->scramble($payload) : $payload;
	}

	private function scramble(string $data): string
	{
		for ($i = 0; $i < strlen($data); $i++) {
			$data[$i] = chr(ord($data[$i]) ^ $this->key);
		}
		return $data;
	}
}

/** A test extension that reverses the payload and owns RSV2 (order-sensitive with ScrambleExtension). */
class ReverseExtension implements IWebSocketExtension
{
	public function getName(): string
	{
		return 'x-reverse';
	}

	public function getReservedRsv(): int
	{
		return IWebSocketExtension::RSV2;
	}

	public function encodeMessage(string $payload): array
	{
		return [strrev($payload), IWebSocketExtension::RSV2];
	}

	public function decodeMessage(string $payload, int $rsv): string
	{
		return ($rsv & IWebSocketExtension::RSV2) !== 0 ? strrev($payload) : $payload;
	}
}

/** An extension that repeats a marked message's payload on receive, as decompression expands one. */
class ExpandingExtension implements IWebSocketExtension
{
	public function __construct(private int $factor)
	{
	}

	public function getName(): string
	{
		return 'x-expand';
	}

	public function getReservedRsv(): int
	{
		return IWebSocketExtension::RSV1;
	}

	public function encodeMessage(string $payload): array
	{
		return [$payload, 0];
	}

	public function decodeMessage(string $payload, int $rsv): string
	{
		return ($rsv & IWebSocketExtension::RSV1) !== 0 ? str_repeat($payload, $this->factor) : $payload;
	}
}

class TWebSocketExtensionTest extends TestCase
{
	/** @return array{0: TWebSocketConnection, 1: TWebSocketConnection, 2: TSocketStream, 3: TSocketStream} */
	private function pair(array $clientExtensions, array $serverExtensions): array
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$client->setExtensions($clientExtensions);
		$server = new TWebSocketConnection($b, false);
		$server->setExtensions($serverExtensions);
		return [$client, $server, $a, $b];
	}

	/** Encodes a frame as a client would send it (masked). */
	private function masked(TWebSocketFrame $frame): string
	{
		return TWebSocketFrameCodec::encode($frame, random_bytes(4));
	}

	public function testSingleExtensionRoundTripsAndSetsRsv1()
	{
		[$client, $server, $a, $b] = $this->pair([new ScrambleExtension()], [new ScrambleExtension()]);
		$client->send('hello extension');
		$wire = $b->read(65536);
		self::assertSame(0x40, ord($wire[0]) & 0x40, 'The extension sets RSV1 on the wire.');
		self::assertSame(['hello extension'], $server->feed($wire));
		$a->close();
		$b->close();
	}

	public function testOrderedPipelineEncodesForwardDecodesReverse()
	{
		// Scramble then Reverse on send; the round-trip only holds if decode runs in reverse order.
		$client = [new ScrambleExtension(), new ReverseExtension()];
		$server = [new ScrambleExtension(), new ReverseExtension()];
		[$clientConn, $serverConn, $a, $b] = $this->pair($client, $server);
		$clientConn->send('pipeline order matters');
		$wire = $b->read(65536);
		self::assertSame(0x60, ord($wire[0]) & 0x60, 'Both RSV1 and RSV2 are set.');
		self::assertSame(['pipeline order matters'], $serverConn->feed($wire));
		$a->close();
		$b->close();
	}

	public function testReservedBitAllowedOnFirstFrameRejectedOnContinuation()
	{
		[, $server, $a, $b] = $this->pair([], [new ScrambleExtension()]);
		// First frame may carry RSV1 (the extension reserves it); a continuation may not.
		$wire = $this->masked(new TWebSocketFrame(TWebSocketOpcode::Text, 'x', false, true))
			. $this->masked(new TWebSocketFrame(TWebSocketOpcode::Continuation, 'y', true, true));
		try {
			$server->feed($wire);
			self::fail('RSV1 on a continuation frame is a protocol error.');
		} catch (TWebSocketException $e) {
			self::assertSame(\Prado\IO\Socket\WebSocket\TWebSocketCloseCode::ProtocolError, $e->getCloseCode());
		}
		$a->close();
		$b->close();
	}

	public function testUnreservedBitIsRejected()
	{
		[, $server, $a, $b] = $this->pair([], [new ScrambleExtension()]);   // reserves RSV1 only
		$frame = new TWebSocketFrame(TWebSocketOpcode::Text, 'x', true, false, true);   // RSV2 set, not reserved
		try {
			$server->feed($this->masked($frame));
			self::fail('A reserved bit no extension owns is a protocol error.');
		} catch (TWebSocketException $e) {
			self::assertSame(\Prado\IO\Socket\WebSocket\TWebSocketCloseCode::ProtocolError, $e->getCloseCode());
		}
		$a->close();
		$b->close();
	}

	public function testUntransformedMessageFromPeerPassesThrough()
	{
		// The peer sends without setting the extension's bit; decode leaves the payload unchanged.
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);   // no extension: plain frame, RSV1 = 0
		$server = new TWebSocketConnection($b, false);
		$server->setExtensions([new ScrambleExtension()]);
		$client->send('plain');
		self::assertSame(['plain'], $server->feed($b->read(65536)));
		$a->close();
		$b->close();
	}

	public function testSetExtensionsRejectsRsvConflict()
	{
		[$a, $b] = TSocketStream::pair();
		$conn = new TWebSocketConnection($a, false);
		try {
			$conn->setExtensions([new ScrambleExtension(0xAA, IWebSocketExtension::RSV1), new ScrambleExtension(0x55, IWebSocketExtension::RSV1)]);
			self::fail('Two extensions may not reserve the same RSV bit.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_extension_rsv_conflict', $e->getErrorCode());
		} finally {
			$a->close();
			$b->close();
		}
		self::assertSame([], $conn->getExtensions(), 'A refused list is not applied.');
	}

	public function testSetExtensionsRejectsNonExtension()
	{
		[$a, $b] = TSocketStream::pair();
		$conn = new TWebSocketConnection($a, false);
		try {
			$conn->setExtensions([new \stdClass()]);
			self::fail('An entry that is not an extension is refused.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_extension_invalid', $e->getErrorCode());
		} finally {
			$a->close();
			$b->close();
		}
		self::assertSame([], $conn->getExtensions(), 'A refused list is not applied.');
	}

	public function testAMessageAnExtensionExpandsPastTheLimitIsRefused()
	{
		// The frame is under MaxMessageSize on the wire; only the extension's output passes it.
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		$server->setExtensions([new ExpandingExtension(50)]);
		$server->setMaxMessageSize(1000);
		$atLimit = $server->feedMessages($this->masked(new TWebSocketFrame(TWebSocketOpcode::Binary, str_repeat('a', 20), true, true)));
		self::assertSame(1000, strlen($atLimit[0]->getPayload()), 'A message that expands to the limit exactly is delivered.');
		self::assertSame(str_repeat('b', 30), $server->feed($this->masked(TWebSocketFrame::binary(str_repeat('b', 30))))[0], 'A message the extension leaves alone is measured as it arrived.');
		try {
			$server->feedMessages($this->masked(new TWebSocketFrame(TWebSocketOpcode::Binary, str_repeat('a', 21), true, true)));
			self::fail('A 21-byte message that expands to 1050 bytes passes the 1000-byte limit.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_message_too_big', $e->getErrorCode());
			self::assertSame(TWebSocketCloseCode::MessageTooBig, $e->getCloseCode(), 'The expanded size fails the connection with 1009.');
		}
		self::assertTrue($server->getIsFailed());

		$blocking = new TWebSocketConnection($b, false);
		$blocking->setExtensions([new ExpandingExtension(50)]);
		$blocking->setMaxMessageSize(1000);
		$a->write($this->masked(new TWebSocketFrame(TWebSocketOpcode::Text, str_repeat('a', 21), true, true)));
		try {
			$blocking->receive();
			self::fail('receive() applies the limit to the expanded message too.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_message_too_big', $e->getErrorCode());
			self::assertSame(TWebSocketCloseCode::MessageTooBig, $e->getCloseCode());
		}

		$unlimited = new TWebSocketConnection($b, false);
		$unlimited->setExtensions([new ExpandingExtension(50)]);
		$unlimited->setMaxMessageSize(0);
		$expanded = $unlimited->feedMessages($this->masked(new TWebSocketFrame(TWebSocketOpcode::Binary, str_repeat('a', 21), true, true)));
		self::assertSame(1050, strlen($expanded[0]->getPayload()), 'A limit of 0 bounds nothing.');
		$a->close();
		$b->close();
	}

	public function testGetExtensionsReturnsConfiguredList()
	{
		[$a, $b] = TSocketStream::pair();
		$conn = new TWebSocketConnection($a, false);
		$extensions = [new ScrambleExtension(), new ReverseExtension()];
		$conn->setExtensions($extensions);
		self::assertSame($extensions, $conn->getExtensions());
		$a->close();
		$b->close();
	}
}
