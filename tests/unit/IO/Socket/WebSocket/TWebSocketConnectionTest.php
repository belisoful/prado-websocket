<?php

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\TStream;

class TWebSocketConnectionTest extends PHPUnit\Framework\TestCase
{
	/** @return array{0: TWebSocketConnection, 1: TWebSocketConnection, 2: TSocketStream, 3: TSocketStream} */
	private function pair(): array
	{
		[$a, $b] = TSocketStream::pair();
		return [new TWebSocketConnection($a, true), new TWebSocketConnection($b, false), $a, $b];
	}

	public function testTextAndBinaryRoundTrip()
	{
		[$client, $server, $a, $b] = $this->pair();
		$client->send('hello server');
		self::assertSame('hello server', $server->receive());
		self::assertSame(TWebSocketOpcode::Text, $server->getLastOpcode());

		$server->sendBinary('BIN');
		self::assertSame('BIN', $client->receive());
		self::assertSame(TWebSocketOpcode::Binary, $client->getLastOpcode());
		$a->close();
		$b->close();
	}

	public function testClientMasksAndServerDoesNot()
	{
		[$ca, $cb] = TSocketStream::pair();
		(new TWebSocketConnection($ca, true))->send('x');
		$wire = $cb->read(8);
		self::assertSame(0x80, ord($wire[1]) & 0x80, 'A client frame is masked.');
		$ca->close();
		$cb->close();

		[$sa, $sb] = TSocketStream::pair();
		(new TWebSocketConnection($sa, false))->send('x');
		$wire = $sb->read(8);
		self::assertSame(0, ord($wire[1]) & 0x80, 'A server frame is not masked.');
		$sa->close();
		$sb->close();
	}

	public function testPingIsAutoPonged()
	{
		[$client, $server, $a, $b] = $this->pair();
		$ping = null;
		$pong = null;
		$server->attachEventHandler('onPing', function ($s, $p) use (&$ping) {
			$ping = $p;
		});
		$client->attachEventHandler('onPong', function ($s, $p) use (&$pong) {
			$pong = $p;
		});
		$client->ping('hb');
		$serverFrame = $server->receiveFrame();          // reads ping, auto-pongs
		$clientFrame = $client->receiveFrame();           // reads the pong
		self::assertSame(TWebSocketOpcode::Ping, $serverFrame->getOpcode());
		self::assertSame('hb', $ping);
		self::assertSame(TWebSocketOpcode::Pong, $clientFrame->getOpcode());
		self::assertSame('hb', $pong);
		$a->close();
		$b->close();
	}

	public function testCloseHandshake()
	{
		[$client, $server, $a, $b] = $this->pair();
		$closeCode = null;
		$server->attachEventHandler('onClose', function ($s, $f) use (&$closeCode) {
			$closeCode = $f->getCloseCode();
		});
		$client->close(1001, 'bye');
		self::assertNull($server->receive(), 'A Close ends receive().');
		self::assertTrue($server->getIsClosed());
		self::assertSame(1001, $closeCode);

		$echo = $client->receiveFrame();                  // the server's echoed Close
		self::assertSame(TWebSocketOpcode::Close, $echo->getOpcode());
		self::assertTrue($client->getIsClosed());
		$a->close();
		$b->close();
	}

	public function testFragmentedMessageReassembled()
	{
		[$client, $server, $a, $b] = $this->pair();
		$client->sendFrame(TWebSocketFrame::text('Hel', false));
		$client->sendFrame(TWebSocketFrame::continuation('lo', true));
		self::assertSame('Hello', $server->receive());
		self::assertSame(TWebSocketOpcode::Text, $server->getLastOpcode());
		$a->close();
		$b->close();
	}

	public function testReceiveReturnsNullAtEof()
	{
		[$client, $server, $a, $b] = $this->pair();
		$a->close();                                       // client gone, no Close frame
		self::assertNull($server->receive());
		self::assertTrue($server->getIsClosed());
		$b->close();
	}

	public function testFeedExtractsMessagesAcrossArbitrarySplits()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$server = new TWebSocketConnection($b, false);
		$client->send('foo');                                  // a whole message
		$client->sendFrame(TWebSocketFrame::text('Hel', false)); // a fragmented message
		$client->sendFrame(TWebSocketFrame::continuation('lo', true));
		$wire = $b->read(8192);

		$half = intdiv(strlen($wire), 2);
		$messages = $server->feed(substr($wire, 0, $half));
		$messages = array_merge($messages, $server->feed(substr($wire, $half)));
		self::assertSame(['foo', 'Hello'], $messages, 'feed() reassembles regardless of byte boundaries.');
		$a->close();
		$b->close();
	}

	public function testFeedHandlesControlFramesAndClose()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$server = new TWebSocketConnection($b, false);
		$pinged = null;
		$server->attachEventHandler('onPing', function ($s, $p) use (&$pinged) {
			$pinged = $p;
		});
		$client->ping('hb');
		$client->close(1000);
		$messages = $server->feed($b->read(8192));

		self::assertSame([], $messages, 'Control frames yield no data messages.');
		self::assertSame('hb', $pinged);
		self::assertTrue($server->getIsClosed(), 'A fed Close marks the connection closed.');
		$a->close();
		$b->close();
	}

	public function testAcceptRunsServerHandshake()
	{
		$req = "GET / HTTP/1.1\r\nHost: ex\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
			. "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
		$s = TStream::fromString($req);
		$conn = TWebSocketConnection::accept($s);
		self::assertInstanceOf(TWebSocketConnection::class, $conn);
		self::assertFalse($conn->getIsClient());
		$s->seek(strlen($req));
		self::assertStringContainsString('101 Switching Protocols', $s->getContents());
		$s->close();
	}
}
