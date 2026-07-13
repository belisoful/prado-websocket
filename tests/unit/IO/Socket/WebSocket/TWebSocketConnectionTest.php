<?php

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
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

	public function testFeedMessagesCarryEachMessagesOwnOpcode()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$server = new TWebSocketConnection($b, false);
		$client->send('a-text');          // Text
		$client->sendBinary('b-binary');  // Binary, arriving in the same read
		$messages = $server->feedMessages($b->read(8192));

		self::assertCount(2, $messages, 'Both messages decode from one read.');
		self::assertSame(TWebSocketOpcode::Text, $messages[0]->getOpcode());
		self::assertSame('a-text', $messages[0]->getPayload());
		self::assertTrue($messages[0]->getIsText());
		self::assertSame(TWebSocketOpcode::Binary, $messages[1]->getOpcode());
		self::assertSame('b-binary', $messages[1]->getPayload());
		self::assertTrue($messages[1]->getIsBinary());

		// getLastOpcode() holds only the last message's opcode after the batch, which is exactly why a
		// per-message handler reads each TWebSocketMessage's own opcode instead.
		self::assertSame(TWebSocketOpcode::Binary, $server->getLastOpcode());
		$a->close();
		$b->close();
	}

	public function testHandlerDispatchesEachMessageWithItsOwnOpcode()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$server = new TWebSocketConnection($b, false);
		$client->send('first');         // Text
		$client->sendBinary('second');  // Binary

		$handler = new class () extends TWebSocketHandler {
			/** @var array<int, array{0: int, 1: string}> */
			public array $seen = [];
			public function onMessage(TWebSocketConnection $connection, string $message, int $opcode): void
			{
				$this->seen[] = [$opcode, $message];
			}
		};
		foreach ($server->feedMessages($b->read(8192)) as $message) {
			$handler->onMessage($server, $message->getPayload(), $message->getOpcode());
		}
		self::assertSame(
			[[TWebSocketOpcode::Text, 'first'], [TWebSocketOpcode::Binary, 'second']],
			$handler->seen,
			'Each message dispatches under its own opcode, not the batch last opcode.',
		);
		$a->close();
		$b->close();
	}

	public function testSendQueuesWithoutBlockingAndDrainsAsThePeerReads()
	{
		[$a, $b] = TSocketStream::pair();
		$a->setBlocking(false);
		$sender = new TWebSocketConnection($a, false);
		$sender->setMaxSendBufferBytes(0);   // unlimited: queue rather than drop, so we can observe the backlog

		$sender->sendBinary(str_repeat('y', 16 * 1024 * 1024));   // far larger than any socket send buffer
		self::assertTrue($sender->hasPendingOutbound(), 'A non-draining peer leaves bytes queued instead of blocking.');
		self::assertGreaterThan(0, $sender->getPendingOutboundLength());

		for ($i = 0; $i < 5000 && $sender->hasPendingOutbound(); $i++) {
			$b->read(65536);              // the peer drains, freeing the send buffer
			$sender->flushOutbound();     // the event loop flushes on writability
		}
		self::assertFalse($sender->hasPendingOutbound(), 'flushOutbound drains the queue as the socket becomes writable.');
		$a->close();
		$b->close();
	}

	public function testSlowReaderOverflowingTheSendBufferIsDropped()
	{
		[$a, $b] = TSocketStream::pair();
		$a->setBlocking(false);
		$sender = new TWebSocketConnection($a, false);
		$sender->setMaxSendBufferBytes(4096);   // a slow reader may not back up more than this

		try {
			$sender->sendBinary(str_repeat('x', 16 * 1024 * 1024));   // the peer never reads
			self::fail('A backlog past the send-buffer limit fails the connection.');
		} catch (TWebSocketException $e) {
			self::assertSame(TWebSocketCloseCode::GoingAway, $e->getCloseCode(), 'An overflowing slow reader is dropped with 1001.');
		}
		$a->close();
		$b->close();
	}

	public function testDefaultSendBufferLimitIsBounded()
	{
		[$a, $b] = TSocketStream::pair();
		$c = new TWebSocketConnection($a, false);
		self::assertSame(TWebSocketConnection::DEFAULT_MAX_SEND_BUFFER, $c->getMaxSendBufferBytes());
		self::assertGreaterThan(0, $c->getMaxSendBufferBytes());
		$a->close();
		$b->close();
	}

	public function testConnectUrlDerivesHostAndTargetFromUrl()
	{
		[$a, $b] = TSocketStream::pair();
		$a->setBlocking(false);
		try {
			TWebSocketConnection::connectUrl($a, 'wss://example.com:8443/chat?room=1');
		} catch (TWebSocketException $e) {
			// No server answers, so verification fails after the request is written.
		}
		$request = $b->read(65536);
		self::assertStringContainsString('GET /chat?room=1 HTTP/1.1', $request, 'The request target comes from the URL.');
		self::assertStringContainsString('Host: example.com:8443', $request, 'The Host header carries the non-default port.');
		$a->close();
		$b->close();
	}

	public function testDrainCloseCompletesWhenPeerAnswers()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$closed = false;
		$client->attachEventHandler('onClose', function () use (&$closed) {
			$closed = true;
		});
		$b->write(TWebSocketFrameCodec::encode(TWebSocketFrame::close(1000)));   // the peer's Close, queued unmasked
		self::assertTrue($client->drainClose(1000), 'drainClose returns true once the connection closes.');
		self::assertTrue($client->getIsClosed());
		self::assertTrue($closed, 'The peer Close raised onClose during the drain.');
		$a->close();
		$b->close();
	}

	public function testDrainCloseReturnsWhenPeerIsGone()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$b->close();                                  // the peer vanished without a Close
		self::assertTrue($client->drainClose(1000, '', 0.2), 'drainClose returns at end of stream rather than hanging.');
		self::assertTrue($client->getIsClosed());
		$a->close();
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
