<?php

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\IWebSocketExtension;
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

class TWebSocketExtensionTest extends PHPUnit\Framework\TestCase
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
		$this->expectException(TWebSocketException::class);
		try {
			$conn->setExtensions([new ScrambleExtension(0xAA, IWebSocketExtension::RSV1), new ScrambleExtension(0x55, IWebSocketExtension::RSV1)]);
		} finally {
			$a->close();
			$b->close();
		}
	}

	public function testSetExtensionsRejectsNonExtension()
	{
		[$a, $b] = TSocketStream::pair();
		$conn = new TWebSocketConnection($a, false);
		$this->expectException(TWebSocketException::class);
		try {
			$conn->setExtensions([new stdClass()]);
		} finally {
			$a->close();
			$b->close();
		}
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
