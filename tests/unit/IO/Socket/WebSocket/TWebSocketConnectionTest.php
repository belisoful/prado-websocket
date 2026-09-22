<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketMessage;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\TStream;

class TWebSocketConnectionTest extends TestCase
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
		$message = $server->receiveMessage();
		self::assertInstanceOf(TWebSocketMessage::class, $message);
		self::assertSame('hello server', $message->getPayload());
		self::assertSame(TWebSocketOpcode::Text, $message->getOpcode());
		self::assertTrue($message->getIsText());

		$server->sendBinary('BIN');
		$message = $client->receiveMessage();
		self::assertSame('BIN', $message->getPayload());
		self::assertTrue($message->getIsBinary());

		$client->send('as string');
		self::assertSame('as string', $server->receive(), 'receive() is the payload form of receiveMessage().');
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

	public function testCloseIsIdempotentAndNoFrameFollowsIt()
	{
		[$client, $server, $a, $b] = $this->pair();
		$client->close(1000, 'done');
		self::assertTrue($client->getIsClosing());
		$client->close(1001, 'again');   // a second Close is a no-op
		self::assertSame(0, $client->ping('late'), 'A Ping after Close is not sent.');
		self::assertSame(0, $client->pong('late'), 'A Pong after Close is not sent.');
		self::assertSame(0, $client->send('late'), 'A data frame after Close is not sent.');
		self::assertSame(0, $client->sendFrame(TWebSocketFrame::text('late')), 'No frame at all follows a Close.');

		$b->setBlocking(false);
		$wire = $b->read(65536);
		$decoded = TWebSocketFrameCodec::tryDecode($wire, true);
		self::assertNotNull($decoded);
		self::assertSame(TWebSocketOpcode::Close, $decoded['frame']->getOpcode());
		self::assertSame(1000, $decoded['frame']->getCloseCode(), 'The first Close is the one on the wire.');
		self::assertSame(strlen($wire), $decoded['length'], 'Exactly one frame was written.');

		self::assertSame([], $server->feedMessages($wire), 'The peer completes the handshake from those bytes.');
		self::assertTrue($server->getIsClosed());
		$a->close();
		$b->close();
	}

	public function testFragmentedMessageReassembled()
	{
		[$client, $server, $a, $b] = $this->pair();
		$client->sendFrame(TWebSocketFrame::text('Hel', false));
		$client->sendFrame(TWebSocketFrame::continuation('lo', true));
		$message = $server->receiveMessage();
		self::assertSame('Hello', $message->getPayload());
		self::assertSame(TWebSocketOpcode::Text, $message->getOpcode());
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

	public function testReceiveFrameOnANonBlockingStreamWithoutDataStaysOpen()
	{
		[$client, $server, $a, $b] = $this->pair();
		$b->setBlocking(false);
		self::assertNull($server->receiveFrame(), 'No frame is available yet.');
		self::assertFalse($server->getIsClosed(), 'An empty non-blocking read is not end of stream.');
		self::assertNull($server->receive(), 'receive() reports no message yet.');
		self::assertFalse($server->getIsClosed());

		$client->send('later');
		self::assertSame('later', $server->receive(), 'The connection keeps serving once bytes arrive.');

		$a->close();                                       // now the stream really ends
		self::assertNull($server->receiveFrame());
		self::assertTrue($server->getIsClosed(), 'End of file closes the connection.');
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

	public function testFeedByteByByteYieldsEveryMessage()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$server = new TWebSocketConnection($b, false);
		$client->send('one');
		$client->send(str_repeat('x', 300));   // a 16-bit extended length
		$client->ping('p');
		$client->sendBinary('two');
		$wire = $b->read(8192);

		$messages = [];
		for ($i = 0; $i < strlen($wire); $i++) {
			foreach ($server->feedMessages($wire[$i]) as $message) {
				$messages[] = [$message->getOpcode(), $message->getPayload()];
			}
		}
		self::assertSame([
			[TWebSocketOpcode::Text, 'one'],
			[TWebSocketOpcode::Text, str_repeat('x', 300)],
			[TWebSocketOpcode::Binary, 'two'],
		], $messages, 'A frame split at any byte is completed when its last byte arrives.');
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
		self::assertSame('b-binary', (string) $messages[1], 'A message stringifies to its payload.');
		$a->close();
		$b->close();
	}

	public function testFeedThousandsOfTinyFramesIsLinear()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		$count = 20000;
		$wire = '';
		for ($i = 0; $i < $count; $i++) {
			$frame = ($i % 5 === 4) ? TWebSocketFrame::ping('') : TWebSocketFrame::text(chr(65 + $i % 26));
			$wire .= TWebSocketFrameCodec::encode($frame, "\x01\x02\x03\x04");   // 6-7 bytes each, masked
		}
		$b->setBlocking(false);
		$a->setBlocking(false);
		$start = microtime(true);
		$messages = $server->feedMessages($wire);
		$elapsed = microtime(true) - $start;

		self::assertCount($count - intdiv($count, 5), $messages, 'Every data frame yields a message; the pings do not.');
		self::assertSame('A', $messages[0]->getPayload());
		self::assertSame(chr(65 + ($count - 2) % 26), $messages[count($messages) - 1]->getPayload(), 'The last data frame is decoded in order.');
		self::assertLessThan(3.0, $elapsed, 'Decoding 20k small frames from one read must not be quadratic.');

		// Consistency: the same wire in two halves yields the same messages, with the split mid-frame.
		$server2 = new TWebSocketConnection($b, false);
		$split = intdiv(strlen($wire), 2) + 3;
		$again = array_merge($server2->feedMessages(substr($wire, 0, $split)), $server2->feedMessages(substr($wire, $split)));
		self::assertSame(array_map(fn ($m) => $m->getPayload(), $messages), array_map(fn ($m) => $m->getPayload(), $again));
		$a->close();
		$b->close();
	}

	public function testFeedRejectsAMalformedHeaderBeforeItsPayloadArrives()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		try {
			$server->feedMessages("\x89\xfe\x00\x80");   // a masked Ping declaring 128 bytes: too long for a control frame
			self::fail('An oversized control frame is a protocol error from its header alone.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_control_frame_too_long', $e->getErrorCode());
		}
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

		$handler = new TWebSocketHandler();
		$seen = [];
		$handler->attachEventHandler('onMessage', function ($connection, $message) use (&$seen) {
			self::assertInstanceOf(TWebSocketMessage::class, $message, 'The onMessage event carries the message object.');
			$seen[] = [$message->getOpcode(), (string) $message];
		});
		foreach ($server->feedMessages($b->read(8192)) as $message) {
			$handler->onMessage($server, $message->getPayload(), $message->getOpcode());
		}
		self::assertSame(
			[[TWebSocketOpcode::Text, 'first'], [TWebSocketOpcode::Binary, 'second']],
			$seen,
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

	public function testSendBufferLimitIsCheckedBeforeTheBytesAreQueued()
	{
		[$a, $b] = TSocketStream::pair();
		$a->setBlocking(false);
		$sender = new TWebSocketConnection($a, false);
		$sender->setMaxSendBufferBytes(64);

		try {
			$sender->sendBinary(str_repeat('x', 100));   // a single message past the limit
			self::fail('A message larger than the send-buffer limit is refused.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_send_buffer_overflow', $e->getErrorCode());
		}
		self::assertSame(0, $sender->getPendingOutboundLength(), 'The refused message was never copied into the queue.');
		self::assertGreaterThan(0, $sender->send('small'), 'The connection still sends within the limit.');
		self::assertSame('small', (new TWebSocketConnection($b, true))->receive());
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

	public function testDrainCloseWaitsForItsTimeoutOnASilentNonBlockingPeer()
	{
		[$a, $b] = TSocketStream::pair();
		$a->setBlocking(false);
		$client = new TWebSocketConnection($a, true);
		$start = microtime(true);
		self::assertFalse($client->drainClose(1000, '', 0.2), 'A silent peer does not complete the close handshake.');
		$elapsed = microtime(true) - $start;
		self::assertGreaterThanOrEqual(0.15, $elapsed, 'The drain keeps waiting until its timeout instead of treating an empty read as end of stream.');
		self::assertLessThan(2.0, $elapsed, 'The drain gives up at its timeout.');
		self::assertFalse($client->getIsClosed(), 'The connection is still closing, not closed.');
		self::assertTrue($client->getIsClosing());

		$b->write(TWebSocketFrameCodec::encode(TWebSocketFrame::close(1000)));   // the peer answers late
		self::assertTrue($client->drainClose(1000, '', 0.5), 'A later drain picks up the peer Close.');
		self::assertTrue($client->getIsClosed());
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
