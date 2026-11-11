<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TInvalidDataValueException;
use Prado\Exceptions\TPhpErrorException;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\IWebSocketExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateExtension;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\Socket\WebSocket\TWebSocketMessage;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\TStream;
use Prado\Prado;

/** A stream whose reads fail the way PRADO's error handler reports an fread() on a reset peer. */
class ResetOnReadStream extends TStream
{
	public function read(int $length): string
	{
		throw new TPhpErrorException(E_NOTICE, 'fread(): Read of ' . $length . ' bytes failed with errno=54 Connection reset by peer', __FILE__, __LINE__);
	}
}

/**
 * A server-side socket whose peer answers each Ping written to it: with a Pong for the first
 * {@see $answers}, then with a Close.
 */
class PingAnsweringStream extends TSocketStream
{
	/** @var ?TSocketStream The peer end that answers. */
	public ?TSocketStream $peer = null;

	/** @var int The Pings written. */
	public int $pings = 0;

	/** @var int The Pings answered with a Pong before the peer closes. */
	public int $answers = 2;

	public function write(string $string): int
	{
		$written = parent::write($string);
		$frame = TWebSocketFrameCodec::tryDecode($string, false)['frame'] ?? null;
		if ($frame !== null && $frame->getOpcode() === TWebSocketOpcode::Ping) {
			$this->pings++;
			$reply = $this->pings <= $this->answers ? TWebSocketFrame::pong($frame->getPayload()) : TWebSocketFrame::close(TWebSocketCloseCode::Normal);
			$this->peer->write(TWebSocketFrameCodec::encode($reply, random_bytes(4)));
		}
		return $written;
	}
}

/** A transport whose peer takes at most {@see $accept} bytes of what is written, and that counts what each write is handed. */
class SlowWriterStream extends TStream
{
	/** @var int The bytes the peer still takes; each write uses it up. */
	public int $accept = 0;

	/** @var int The total length of the strings handed to write(). */
	public int $offered = 0;

	/** @var string The bytes the peer took, in order. */
	public string $taken = '';

	public function write(string $string): int
	{
		$this->offered += strlen($string);
		$take = min($this->accept, strlen($string));
		$this->accept -= $take;
		$this->taken .= substr($string, 0, $take);
		return $take;
	}
}

/**
 * A server-side socket whose peer never stops sending: before each read the peer writes one more
 * piece (a whole frame, or the next byte of one long frame) and the test clock moves forward.  After
 * {@see $limit} reads the peer ends the stream, so a reader that ignores its deadline still returns.
 */
class ChattyPeerStream extends TSocketStream
{
	/** @var ?TSocketStream The peer end. */
	public ?TSocketStream $peer = null;

	/** @var ?TTestMonotonicClock The clock each read advances. */
	public ?TTestMonotonicClock $clock = null;

	/** @var float The seconds each read takes on the clock. */
	public float $step = 0.1;

	/** @var int The reads made. */
	public int $reads = 0;

	/** @var int The reads after which the peer ends the stream. */
	public int $limit = 400;

	/** @var string The frame the peer sends before each read. */
	public string $piece = '';

	/** @var string A frame the peer sends one byte of before each read, in place of {@see $piece}. */
	public string $trickle = '';

	/** @var array<int, array{0: int, 1: int}> The read timeouts set, as seconds and microseconds. */
	public array $timeouts = [];

	public function setTimeout(int $seconds, int $microseconds = 0): bool
	{
		$this->timeouts[] = [$seconds, $microseconds];
		return parent::setTimeout($seconds, $microseconds);
	}

	public function read(int $length): string
	{
		$this->reads++;
		$this->clock?->advance($this->step);
		if ($this->peer !== null) {
			if ($this->reads > $this->limit) {
				$this->peer->close();
				$this->peer = null;
			} elseif ($this->trickle !== '') {
				$this->peer->write($this->trickle[($this->reads - 1) % strlen($this->trickle)]);
			} else {
				$this->peer->write($this->piece);
			}
		}
		return parent::read($length);
	}
}

/** A client socket that lets an in-process server answer once the client has written its request. */
class AnsweredClientStream extends TSocketStream
{
	/** @var ?\Closure Runs once, after the first write. */
	public ?\Closure $answer = null;

	public function write(string $string): int
	{
		$written = parent::write($string);
		if ($this->answer !== null) {
			$answer = $this->answer;
			$this->answer = null;
			$answer();
		}
		return $written;
	}
}

/** An extension that changes nothing and counts the messages it encodes. */
class CountingExtension implements IWebSocketExtension
{
	/** @var int The messages encoded. */
	public int $encoded = 0;

	public function getName(): string
	{
		return 'x-counting';
	}

	public function getReservedRsv(): int
	{
		return 0;
	}

	public function encodeMessage(string $payload): array
	{
		$this->encoded++;
		return [$payload, 0];
	}

	public function decodeMessage(string $payload, int $rsv): string
	{
		return $payload;
	}
}

class TWebSocketConnectionTest extends TestCase
{
	private const KEY = "\x01\x02\x03\x04";

	/** @var string[] Temporary certificate files to remove. */
	private array $tempFiles = [];

	protected function tearDown(): void
	{
		foreach ($this->tempFiles as $file) {
			@unlink($file);
		}
	}

	/** @return array{0: TWebSocketConnection, 1: TWebSocketConnection, 2: TSocketStream, 3: TSocketStream} */
	private function pair(): array
	{
		[$a, $b] = TSocketStream::pair();
		return [new TWebSocketConnection($a, true), new TWebSocketConnection($b, false), $a, $b];
	}

	/**
	 * Returns whether the run collects code coverage.  Instrumented PHP runs many times slower, and
	 * Xdebug's path coverage costs time quadratic in the branches a single call executes, so the
	 * performance tests shrink their load and assert no wall-clock bound under coverage.
	 * @return bool Whether a coverage driver is collecting.
	 */
	private static function isInstrumented(): bool
	{
		if (function_exists('xdebug_code_coverage_started') && xdebug_code_coverage_started()) {
			return true;
		}
		// PCOV has no query for whether it is collecting; PHPUnit's runner knows whether it started a driver.
		$runner = '\PHPUnit\Runner\CodeCoverage';
		return class_exists($runner) && method_exists($runner, 'instance') && method_exists($runner, 'isActive') && $runner::instance()->isActive();
	}

	/**
	 * Builds a server-side socket whose peer keeps sending, with the clock its reads advance.
	 * @return array{0: ChattyPeerStream, 1: TSocketStream, 2: TTestMonotonicClock} The server transport, the peer end and the clock.
	 */
	private function chattyPeer(): array
	{
		[$serverEnd, $clientEnd] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
		$transport = new ChattyPeerStream();
		$transport->attachResource($serverEnd, true);
		$peer = new TSocketStream();
		$peer->attachResource($clientEnd, true);
		$transport->peer = $peer;
		$transport->clock = new TTestMonotonicClock(100);
		return [$transport, $peer, $transport->clock];
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
		self::assertNull($server->receiveMessage(), 'A closed connection yields no message and reads nothing.');
		self::assertFalse($server->getIsFailed(), 'A clean end of stream is not a failure.');
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
		$b->setBlocking(false);
		$a->setBlocking(false);
		$instrumented = self::isInstrumented();
		$count = $instrumented ? 200 : 40000;
		$frames = [];
		for ($i = 0; $i < $count; $i++) {
			$frame = ($i % 5 === 4) ? TWebSocketFrame::ping('') : TWebSocketFrame::text(str_repeat(chr(65 + $i % 26), 100));
			$frames[] = TWebSocketFrameCodec::encode($frame, self::KEY);   // 6 or 106 bytes each, masked
		}
		$wire = implode('', $frames);   // 3.4 MiB

		// The baseline: the same frames fed one per call, which costs time linear in their count however the buffer is kept.
		$oneByOne = new TWebSocketConnection($b, false);
		$expected = [];
		$start = microtime(true);
		foreach ($frames as $frame) {
			foreach ($oneByOne->feedMessages($frame) as $message) {
				$expected[] = $message->getPayload();
			}
		}
		$baseline = microtime(true) - $start;
		while ($a->read(65536) !== '') {
			// drain the Pongs, so the measured run starts with the same empty socket
		}

		$server = new TWebSocketConnection($b, false);
		$start = microtime(true);
		$messages = $server->feedMessages($wire);
		$elapsed = microtime(true) - $start;

		self::assertCount($count - intdiv($count, 5), $messages, 'Every data frame yields a message; the pings do not.');
		self::assertSame(str_repeat('A', 100), $messages[0]->getPayload());
		self::assertSame(str_repeat(chr(65 + ($count - 2) % 26), 100), $messages[count($messages) - 1]->getPayload(), 'The last data frame is decoded in order.');
		self::assertSame($expected, array_map(fn ($m) => $m->getPayload(), $messages), 'One read yields what the same frames yield one by one.');
		if (!$instrumented) {
			// Re-copying the rest of the read per frame is about 68 GiB of memcpy here, seconds against the baseline's tenth of one.
			self::assertLessThan(4 * $baseline + 0.25, $elapsed, 'Decoding 40k small frames from one read costs about what feeding them one by one does, not time quadratic in their count.');
		}

		// Consistency: the same wire in two halves yields the same messages, with the split mid-frame.
		$server2 = new TWebSocketConnection($b, false);
		$split = intdiv(strlen($wire), 2) + 3;
		$again = array_merge($server2->feedMessages(substr($wire, 0, $split)), $server2->feedMessages(substr($wire, $split)));
		self::assertSame($expected, array_map(fn ($m) => $m->getPayload(), $again));
		$a->close();
		$b->close();
	}

	public function testFeedALargeFrameInSmallPiecesIsLinear()
	{
		[$a, $b] = TSocketStream::pair();
		$instrumented = self::isInstrumented();
		$payload = str_repeat('0123456789abcdef', $instrumented ? 1 << 16 : 1 << 19);   // 8 MiB; 1 MiB under coverage
		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::binary($payload), self::KEY)
			. TWebSocketFrameCodec::encode(TWebSocketFrame::text('next'), "\x05\x06\x07\x08");

		// The baseline: the whole wire in one read, the least the frame costs to decode.
		$atOnce = new TWebSocketConnection($b, false);
		$atOnce->setMaxMessageSize(10 << 20);
		$start = microtime(true);
		$whole = $atOnce->feedMessages($wire);
		$baseline = microtime(true) - $start;
		self::assertCount(2, $whole);
		unset($whole, $atOnce);

		$server = new TWebSocketConnection($b, false);
		$server->setMaxMessageSize(10 << 20);
		$trickle = 256 << 10;
		$start = microtime(true);
		$messages = $server->feedMessages(substr($wire, 0, -$trickle));   // most of the frame at once
		foreach (str_split(substr($wire, -$trickle), 64) as $piece) {   // then 4096 reads of 64 bytes: re-copying the buffered frame per read is ~32 GiB of memcpy
			array_push($messages, ...$server->feedMessages($piece));
		}
		$elapsed = microtime(true) - $start;

		self::assertCount(2, $messages);
		self::assertSame($payload, $messages[0]->getPayload(), 'The frame is reassembled from its pieces.');
		self::assertSame('next', $messages[1]->getPayload(), 'The frame after it, split across the same pieces, follows.');
		if (!$instrumented) {
			self::assertLessThan(8 * $baseline + 0.5, $elapsed, 'A frame trickled in small reads costs a small multiple of decoding it from one read, not time quadratic in its size.');
		}
		$a->close();
		$b->close();
	}

	public function testAFrameTrickledInSingleBytesIsHeldInProportionToItsSize()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		$size = self::isInstrumented() ? 1 << 11 : 1 << 20;   // 1 MiB; 2 KiB under coverage, where each call costs about a millisecond
		$payload = str_repeat('0123456789abcdef', $size >> 4);
		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::binary($payload), self::KEY);
		$last = strlen($wire) - 1;

		$yielded = 0;
		$before = memory_get_usage();
		for ($i = 0; $i < $last; $i++) {
			$yielded += count($server->feedMessages($wire[$i]));   // one byte per read, as a peer sending 1-byte segments delivers it
		}
		$held = memory_get_usage() - $before;

		self::assertSame(0, $yielded, 'The frame is incomplete until its last byte.');
		self::assertLessThan(2 * $size + 16384, $held, 'A frame trickled in 1-byte reads is held in about its own size: one array entry per read costs 16 to 64 times that.');
		$messages = $server->feedMessages($wire[$last]);
		self::assertCount(1, $messages);
		self::assertSame($payload, $messages[0]->getPayload(), 'The frame is reassembled from its single bytes.');
		$a->close();
		$b->close();
	}

	public function testAFrameTrickledInUnevenPiecesIsReassembled()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		$payload = random_bytes(300000);
		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::binary($payload), self::KEY)
			. TWebSocketFrameCodec::encode(TWebSocketFrame::text('next'), self::KEY);
		$messages = [];
		$offset = 0;
		foreach ([3, 9, 1, 70000, 1, 65536, 5, 131072, 17] as $length) {   // pieces under, at and over the 64 KiB a held piece grows to
			array_push($messages, ...$server->feedMessages(substr($wire, $offset, $length)));
			$offset += $length;
		}
		self::assertSame([], $messages, 'The frame is incomplete so far.');
		array_push($messages, ...$server->feedMessages(substr($wire, $offset)));
		self::assertCount(2, $messages);
		self::assertSame($payload, $messages[0]->getPayload(), 'Pieces of every size join in order.');
		self::assertSame('next', $messages[1]->getPayload());
		$a->close();
		$b->close();
	}

	public function testFeedAfterThePeersCloseDiscardsTheBytes()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		$close = TWebSocketFrameCodec::encode(TWebSocketFrame::close(TWebSocketCloseCode::Normal), "\x01\x02\x03\x04");
		self::assertSame([], $server->feedMessages($close . str_repeat("\x82", 1 << 20)));
		self::assertTrue($server->getIsClosed());
		$before = memory_get_usage();
		for ($i = 0; $i < 16; $i++) {
			self::assertSame([], $server->feedMessages(str_repeat("\x82", 1 << 20)), 'Bytes after the peer\'s Close yield nothing.');
		}
		self::assertLessThan(1 << 20, memory_get_usage() - $before, 'Bytes after the peer\'s Close are not buffered.');
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

	public function testAMalformedFrameFailsTheConnectionAndLaterBytesAreDiscarded()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		self::assertFalse($server->getIsFailed());
		$server->close(TWebSocketCloseCode::PolicyViolation, 'refused');   // the application closed first, as a refusal in onOpen does
		$one = TWebSocketFrameCodec::encode(TWebSocketFrame::text('one'), self::KEY);
		try {
			$server->feedMessages($one . "\x81\x05hello" . $one);   // a message, then an unmasked frame, then more
			self::fail('An unmasked frame is a protocol error.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_frame_not_masked', $e->getErrorCode());
			self::assertCount(1, $e->getDecodedMessages(), 'The message before the malformed frame still travels with the failure.');
			self::assertSame('one', $e->getDecodedMessages()[0]->getPayload());
		}
		self::assertTrue($server->getIsFailed(), 'The connection remembers that reading failed.');
		self::assertFalse($server->getIsClosed(), 'A failed connection is not closed: its Close is still to drain.');

		$junk = str_repeat("\0", 1 << 20);
		$before = memory_get_usage();
		for ($i = 0; $i < 16; $i++) {
			self::assertSame([], $server->feedMessages($junk), 'A later feed reports the failure no second time and yields nothing.');
		}
		self::assertLessThan(1 << 20, memory_get_usage() - $before, 'The bytes behind a malformed frame are not buffered.');

		$close = TWebSocketFrameCodec::encode(TWebSocketFrame::close(TWebSocketCloseCode::Normal), self::KEY);
		self::assertSame([], $server->feedMessages($one . $close), 'Nothing behind the failure is parsed, a valid frame included.');
		self::assertSame([], $server->feed($one), 'feed() discards the same way.');
		self::assertFalse($server->getIsClosed(), 'The peer\'s Close behind a protocol error is not processed.');
		$a->close();
		$b->close();
	}

	public function testAMalformedMessageFailsTheConnectionToo()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		$text = TWebSocketFrameCodec::encode(TWebSocketFrame::text('fine'), self::KEY);
		try {
			$server->feedMessages(TWebSocketFrameCodec::encode(TWebSocketFrame::text("\xff\xfe"), self::KEY) . $text);
			self::fail('Invalid UTF-8 in a Text message fails the connection.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_text_not_utf8', $e->getErrorCode());
		}
		self::assertTrue($server->getIsFailed(), 'A message-level error fails reading as a frame-level one does.');
		self::assertSame([], $server->feedMessages($text), 'The frames behind the bad message are dropped with it.');

		$partial = new TWebSocketConnection($b, false);
		self::assertSame([], $partial->feedMessages(TWebSocketFrameCodec::encode(TWebSocketFrame::text('Hel', false), self::KEY)));
		try {
			$partial->feedMessages($text);   // a new Text frame inside an unfinished fragmented message
			self::fail('A data frame inside a fragmented message is a protocol error.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_fragment_incomplete', $e->getErrorCode());
		}
		self::assertSame([], $partial->feedMessages(TWebSocketFrameCodec::encode(TWebSocketFrame::continuation('lo'), self::KEY)), 'The message being reassembled is dropped with the failure.');
		$a->close();
		$b->close();
	}

	public function testAProtocolErrorOnReceiveFailsTheConnectionOnce()
	{
		[$a, $b] = TSocketStream::pair();
		$b->setBlocking(false);
		$server = new TWebSocketConnection($b, false);
		$text = TWebSocketFrameCodec::encode(TWebSocketFrame::text('fine'), self::KEY);
		$a->write("\x81\x05hello" . $text);   // an unmasked frame, then a valid one
		try {
			$server->receive();
			self::fail('An unmasked frame is a protocol error.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_frame_not_masked', $e->getErrorCode());
		}
		self::assertTrue($server->getIsFailed());
		self::assertNull($server->receive(), 'A failed connection reads nothing more and throws no second time.');
		self::assertNull($server->receiveFrame());
		self::assertFalse($server->getReadTimedOut(), 'The null of a failed connection is not a read timeout.');
		self::assertSame([], $server->feedMessages($text));

		$a->close();
		$b->close();

		[$c, $d] = TSocketStream::pair();
		$d->setBlocking(false);
		$utf8 = new TWebSocketConnection($d, false);
		$c->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text("\xc3\x28"), self::KEY) . $text);
		try {
			$utf8->receive();
			self::fail('Invalid UTF-8 fails receive().');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_text_not_utf8', $e->getErrorCode());
		}
		self::assertTrue($utf8->getIsFailed(), 'A message-level error on receive() fails reading too.');
		self::assertNull($utf8->receive(), 'The valid frame behind it is not read.');
		$c->close();
		$d->close();
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

		$sender->sendBinary(str_repeat('y', self::isInstrumented() ? 1 << 20 : 16 << 20));   // far larger than any socket send buffer
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
		if (!self::isInstrumented()) {
			self::assertLessThan(2.0, $elapsed, 'The drain gives up at its timeout.');
		}
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

	/**
	 * Reads every frame the server end wrote to its peer, without blocking.
	 * @param TSocketStream $peer The client end.
	 * @return TWebSocketFrame[] The frames, in order.
	 */
	private static function serverFrames(TSocketStream $peer): array
	{
		$peer->setBlocking(false);
		$wire = '';
		while (($chunk = $peer->read(65536)) !== '') {
			$wire .= $chunk;
		}
		$frames = [];
		while (($decoded = TWebSocketFrameCodec::tryDecode($wire, false)) !== null) {
			$frames[] = $decoded['frame'];
			$wire = substr($wire, $decoded['length']);
		}
		return $frames;
	}

	public function testSendToAResetPeerFailsTheWriteUnderPradosErrorHandler()
	{
		[$client, $server, $a, $b] = $this->pair();
		$a->close();                                       // the peer is gone: a write raises EPIPE
		set_error_handler([Prado::class, 'phpErrorHandler']);   // PRADO turns the fwrite() notice into a TPhpErrorException
		try {
			for ($i = 0; $i < 2; $i++) {
				try {
					$server->send(str_repeat('x', 100));
					self::fail('A write to a reset peer fails.');
				} catch (TWebSocketException $e) {
					self::assertSame('websocket_write_failed', $e->getErrorCode(), 'The documented write failure is thrown, not the PHP notice.');
				}
				self::assertTrue($server->getIsClosed(), 'The failed write marks the connection closed.');
				self::assertSame(0, $server->getPendingOutboundLength(), 'The queue does not grow across failed sends.');
			}
		} finally {
			restore_error_handler();
		}
		$b->close();
	}

	public function testReceiveFromAResetPeerFailsTheReadUnderPradosErrorHandler()
	{
		$server = new TWebSocketConnection(new ResetOnReadStream(fopen('php://memory', 'r+')), false);
		try {
			$server->receive();
			self::fail('A read from a reset peer fails.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_read_failed', $e->getErrorCode());
		}
		self::assertTrue($server->getIsClosed(), 'The failed read marks the connection closed.');
		self::assertFalse($server->getReadTimedOut(), 'A failed read is not a timeout.');
	}

	public function testAPartialFrameOnANonBlockingStreamIsBufferedUntilItCompletes()
	{
		[$a, $b] = TSocketStream::pair();
		$b->setBlocking(false);
		$server = new TWebSocketConnection($b, false);
		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::text('hello world'), "\x01\x02\x03\x04");   // 17 bytes
		$a->write(substr($wire, 0, 3));
		self::assertNull($server->receive(), 'A partial frame is not a message yet.');
		self::assertFalse($server->getIsClosed(), 'A partial frame does not close the connection.');
		self::assertFalse($server->getReadTimedOut(), 'An empty non-blocking read is not a timeout.');
		$a->write(substr($wire, 3, 7));
		self::assertNull($server->receive(), 'Still partial.');
		$a->write(substr($wire, 10) . TWebSocketFrameCodec::encode(TWebSocketFrame::text('next'), "\x05\x06\x07\x08"));
		self::assertSame('hello world', $server->receive(), 'The buffered bytes complete the frame.');
		self::assertSame('next', $server->receive(), 'The frame behind it is read intact.');
		$a->close();
		$b->close();
	}

	public function testABlockingReadTimeoutKeepsTheConnectionAndItsPartialFrame()
	{
		[$a, $b] = TSocketStream::pair();
		$b->setTimeout(0, 20000);
		$server = new TWebSocketConnection($b, false);
		self::assertNull($server->receive(), 'A read timeout yields no message.');
		self::assertTrue($server->getReadTimedOut(), 'The null reports a read timeout.');
		self::assertFalse($server->getIsClosed(), 'A read timeout does not close the connection.');

		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::text('later'), "\x01\x02\x03\x04");
		$a->write(substr($wire, 0, 4));
		self::assertNull($server->receive(), 'A frame cut by the timeout stays buffered.');
		self::assertTrue($server->getReadTimedOut());
		$a->write(substr($wire, 4));
		self::assertSame('later', $server->receive(), 'The connection keeps serving once the rest arrives.');
		self::assertFalse($server->getReadTimedOut());
		$a->close();
		$b->close();
	}

	public function testTheStreamEndingInsideAFrameFailsAndCloses()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		$a->write(substr(TWebSocketFrameCodec::encode(TWebSocketFrame::text('hello'), "\x01\x02\x03\x04"), 0, 5));
		$a->close();
		try {
			$server->receive();
			self::fail('A frame cut short by the end of the stream fails.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_frame_incomplete', $e->getErrorCode());
		}
		self::assertTrue($server->getIsClosed());
		$b->close();
	}

	public function testReceiveRefusesAnOversizedFrameFromItsHeader()
	{
		[$a, $b] = TSocketStream::pair();
		$b->setBlocking(false);
		$server = new TWebSocketConnection($b, false);
		$server->setMaxMessageSize(100);
		$a->write("\x82\xfe\x01\x00\x01\x02\x03\x04");   // a masked Binary frame declaring 256 bytes; no payload sent
		try {
			$server->receive();
			self::fail('A frame over the limit is refused before its payload is read.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_message_too_big', $e->getErrorCode());
		}
		$a->close();
		$b->close();
	}

	public function testReceiveAfterFeedContinuesFromTheSameBuffer()
	{
		[$a, $b] = TSocketStream::pair();
		$b->setBlocking(false);
		$server = new TWebSocketConnection($b, false);
		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::text(str_repeat('a', 200)), "\x01\x02\x03\x04");
		self::assertSame([], $server->feedMessages(substr($wire, 0, 10)));
		self::assertSame([], $server->feedMessages(substr($wire, 10, 10)), 'The frame of known length is collected in pieces.');
		$a->write(substr($wire, 20));
		self::assertSame(str_repeat('a', 200), $server->receive(), 'receive() joins the collected pieces with the bytes it reads.');
		$a->close();
		$b->close();
	}

	public function testFeedMessagesHandsTheDecodedMessagesToTheFailure()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		$key = "\x01\x02\x03\x04";
		$wire = TWebSocketFrameCodec::encode(TWebSocketFrame::text('one'), $key)
			. TWebSocketFrameCodec::encode(TWebSocketFrame::binary('two'), $key)
			. TWebSocketFrameCodec::encode(TWebSocketFrame::continuation('orphan'), $key);   // a continuation with no message
		try {
			$server->feedMessages($wire);
			self::fail('The malformed frame fails the read.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_continuation_unexpected', $e->getErrorCode());
			$messages = $e->getDecodedMessages();
			self::assertCount(2, $messages, 'The messages before the malformed frame travel with the failure.');
			self::assertSame('one', $messages[0]->getPayload());
			self::assertTrue($messages[1]->getIsBinary());
			self::assertSame('two', $messages[1]->getPayload());
		}

		$fresh = new TWebSocketConnection($b, false);
		try {
			$fresh->feedMessages(TWebSocketFrameCodec::encode(TWebSocketFrame::continuation('orphan'), $key));
			self::fail('A malformed first frame fails.');
		} catch (TWebSocketException $e) {
			self::assertSame([], $e->getDecodedMessages(), 'With nothing decoded first the failure carries no messages.');
		}
		$a->close();
		$b->close();
	}

	public function testCloseRefusesABadCodeOrReasonWithoutClosing()
	{
		[$client, $server, $a, $b] = $this->pair();
		foreach ([[TWebSocketCloseCode::Normal, str_repeat('r', 124)], [TWebSocketCloseCode::NoStatusReceived, ''], [TWebSocketCloseCode::Normal, "\xff"]] as [$code, $reason]) {
			try {
				$server->close($code, $reason);
				self::fail('A Close that cannot be built is refused.');
			} catch (TInvalidDataValueException $e) {
				self::assertFalse($server->getIsClosing(), 'A refused Close leaves the connection open.');
				self::assertFalse($server->getIsCloseInitiator());
			}
		}
		self::assertSame(0, $server->getPendingOutboundLength(), 'Nothing was queued.');
		$server->close(TWebSocketCloseCode::Normal, str_repeat('r', 123));
		self::assertTrue($server->getIsClosing(), 'A valid Close still closes.');
		self::assertSame(TWebSocketCloseCode::Normal, $client->receiveFrame()->getCloseCode());
		$a->close();
		$b->close();
	}

	public function testDrainCloseMeasuresItsTimeoutOnTheMonotonicClock()
	{
		[$a, $b] = TSocketStream::pair();
		$a->setBlocking(false);
		$client = new TWebSocketConnection($a, true);
		$clock = new class () extends TTestMonotonicClock {
			public int $readings = 0;

			protected function hrtime(): int
			{
				$this->readings++;
				$this->advance(0.1);   // every reading moves 100 ms
				return parent::hrtime();
			}
		};
		$client->setMonotonicClock($clock);
		self::assertFalse($client->drainClose(1000, '', 0.25), 'A silent peer does not complete the close handshake.');
		self::assertGreaterThan(0, $clock->readings, 'The drain deadline is read from the monotonic clock.');
		self::assertLessThanOrEqual(4, $clock->readings, 'The deadline passes once the monotonic readings cover the timeout.');
		$a->close();
		$b->close();
	}

	public function testHandleConnectionPingsAnIdlePeerAndClosesAnUnresponsiveOne()
	{
		[$a, $b] = TSocketStream::pair();
		$b->setTimeout(0, 20000);                         // the idle interval: each blocking read waits 20 ms
		$server = new TWebSocketConnection($b, false);
		$handler = new TWebSocketHandler();
		$errors = [];
		$handler->attachEventHandler('onError', function ($sender, $error) use (&$errors) {
			$errors[] = $error;
		});
		$handler->handleConnection($server);
		self::assertSame([], $errors, 'An idle peer is not an error.');

		$frames = self::serverFrames($a);
		self::assertCount(2, $frames);
		self::assertSame(TWebSocketOpcode::Ping, $frames[0]->getOpcode(), 'The first idle interval pings the peer.');
		self::assertSame(TWebSocketOpcode::Close, $frames[1]->getOpcode());
		self::assertSame(TWebSocketCloseCode::GoingAway, $frames[1]->getCloseCode(), 'A second idle interval with no answer closes with 1001.');
		$a->close();
		$b->close();
	}

	public function testHandleConnectionKeepsAPeerThatAnswersEachPing()
	{
		[$serverEnd, $clientEnd] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
		$transport = new PingAnsweringStream();
		$transport->attachResource($serverEnd, true);
		$transport->setTimeout(0, 20000);
		$peer = new TSocketStream();
		$peer->attachResource($clientEnd, true);
		$transport->peer = $peer;
		$server = new TWebSocketConnection($transport, false);
		$handler = new TWebSocketHandler();
		$errors = [];
		$handler->attachEventHandler('onError', function ($sender, $error) use (&$errors) {
			$errors[] = $error;
		});
		$handler->handleConnection($server);
		self::assertSame([], $errors);
		self::assertSame(3, $transport->pings, 'Each answered Ping keeps the connection for another idle interval.');
		self::assertTrue($server->getIsClosed(), 'The peer\'s Close ends the loop.');
		$frames = self::serverFrames($peer);
		self::assertSame(TWebSocketCloseCode::Normal, end($frames)->getCloseCode(), 'The server echoes the peer\'s Close rather than dropping it as unresponsive.');
		$transport->close();
		$peer->close();
	}

	// ---- The send queue ---------------------------------------------------------

	public function testFlushOutboundHandsASlowReaderABoundedSliceOfTheBacklog()
	{
		$stream = new SlowWriterStream(fopen('php://memory', 'r+'));
		$sender = new TWebSocketConnection($stream, false);
		$message = str_repeat('x', self::isInstrumented() ? 1 << 16 : 1 << 20);   // 1 MiB; 64 KiB under coverage
		$wire = '';
		for ($i = 0; $i < 4; $i++) {
			$sender->sendBinary($message);   // the peer takes nothing yet: 4 MiB queue
			$wire .= TWebSocketFrameCodec::encode(TWebSocketFrame::binary($message));
		}
		$queued = strlen($wire);
		self::assertSame($queued, $sender->getPendingOutboundLength());
		self::assertSame($queued, $sender->getQueuedLength());

		$stream->offered = 0;
		$events = 0;
		$mismatches = 0;
		while ($sender->hasPendingOutbound()) {
			$stream->accept = 4096;   // the peer takes 4 KiB per writable event
			$queued -= $sender->flushOutbound();
			$events++;
			$mismatches += (int) ($sender->getPendingOutboundLength() !== $queued || $sender->getQueuedLength() !== $queued);
		}
		self::assertSame(0, $queued, 'Every flush reports the bytes it wrote.');
		self::assertSame(0, $mismatches, 'The queued length is exact after every partial write, whatever the send offset.');
		self::assertSame(0, $sender->flushOutbound(), 'An empty queue flushes nothing.');
		self::assertSame($wire, $stream->taken, 'The peer receives the queue in order, byte for byte.');
		// Each event hands over at most two 64 KiB slices; the queue itself is handed over, uncopied, when its offset returns to 0.
		self::assertLessThanOrEqual($events * 2 * 65536 + 2 * strlen($wire), $stream->offered, 'A writable event hands the transport a bounded slice. Handing it the whole backlog copies the backlog per event, 4 GiB in all for 4 MiB.');
	}

	public function testSendBufferAccountingIsExactBehindTheSendOffset()
	{
		$stream = new SlowWriterStream(fopen('php://memory', 'r+'));
		$sender = new TWebSocketConnection($stream, false);
		$sender->setMaxSendBufferBytes(1000);
		$overflows = function (string $payload) use ($sender): bool {
			try {
				$sender->sendBinary($payload);
				return false;
			} catch (TWebSocketException $e) {
				self::assertSame('websocket_send_buffer_overflow', $e->getErrorCode());
				return true;
			}
		};
		$sender->sendBinary(str_repeat('a', 498));   // a 4-byte header: 502 bytes queued
		$stream->accept = 100;
		self::assertSame(100, $sender->flushOutbound(), 'The peer takes 100 bytes: the written prefix is under half of the queue and stays behind the offset.');
		self::assertSame(402, $sender->getPendingOutboundLength());
		self::assertTrue($sender->hasPendingOutbound());

		self::assertTrue($overflows(str_repeat('b', 595)), '402 queued and a 599-byte frame pass the 1000-byte limit by one.');
		self::assertSame(402, $sender->getPendingOutboundLength(), 'A refused frame is not queued.');
		self::assertFalse($overflows(str_repeat('b', 594)), '402 queued and a 598-byte frame fill the limit exactly.');
		self::assertSame(1000, $sender->getQueuedLength());
		self::assertTrue($overflows(''), 'A full queue takes no further frame.');

		$stream->accept = 700;
		self::assertSame(700, $sender->flushOutbound(), 'The written prefix now passes half of the queue, which is compacted.');
		self::assertSame(300, $sender->getPendingOutboundLength());
		self::assertTrue($overflows(str_repeat('c', 697)), '300 queued and a 701-byte frame pass the limit by one.');
		self::assertFalse($overflows(str_repeat('c', 696)), '300 queued and a 700-byte frame fill the limit exactly.');

		$stream->accept = PHP_INT_MAX;
		self::assertSame(1000, $sender->flushOutbound());
		self::assertFalse($sender->hasPendingOutbound());
		self::assertSame(0, $sender->getQueuedLength());
		$expected = TWebSocketFrameCodec::encode(TWebSocketFrame::binary(str_repeat('a', 498)))
			. TWebSocketFrameCodec::encode(TWebSocketFrame::binary(str_repeat('b', 594)))
			. TWebSocketFrameCodec::encode(TWebSocketFrame::binary(str_repeat('c', 696)));
		self::assertSame($expected, $stream->taken, 'The frames reach the peer whole and in order across the offset and the compaction.');
	}

	public function testAFullSendBufferRefusesAMessageBeforeAnExtensionEncodesIt()
	{
		$stream = new SlowWriterStream(fopen('php://memory', 'r+'));
		$sender = new TWebSocketConnection($stream, false);
		$extension = new CountingExtension();
		$sender->setExtensions([$extension]);
		$sender->setMaxSendBufferBytes(100);

		$sender->sendBinary(str_repeat('x', 90));   // 92 bytes queued
		self::assertSame(1, $extension->encoded);
		try {
			$sender->sendBinary(str_repeat('x', 20));   // room for a frame, though not for this one: its size is known only once encoded
			self::fail('A frame past the limit is refused.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_send_buffer_overflow', $e->getErrorCode());
		}
		self::assertSame(2, $extension->encoded);
		$sender->sendBinary(str_repeat('x', 6));   // 100 bytes queued: the buffer is full
		self::assertSame(3, $extension->encoded);
		try {
			$sender->send('');
			self::fail('A full send buffer takes no message.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_send_buffer_overflow', $e->getErrorCode());
			self::assertSame(TWebSocketCloseCode::GoingAway, $e->getCloseCode());
		}
		self::assertSame(3, $extension->encoded, 'With no room for the shortest frame, the message is refused before the extension sees it.');
		self::assertSame(100, $sender->getPendingOutboundLength());
	}

	public function testASendRefusedForOverflowLeavesTheDeflateStreamDecodable()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($a, false);
		$server->setExtensions([new TPermessageDeflateExtension()]);
		$server->setMaxSendBufferBytes(600);
		$client = new TWebSocketConnection($b, true);
		$client->setExtensions([new TPermessageDeflateExtension()]);
		$server->send('before');
		self::assertSame('before', $client->receive());

		$big = bin2hex(random_bytes(1000));   // 2000 hex digits compress to about 1000 bytes: past the limit, known only once compressed
		try {
			$server->send($big);
			self::fail('A message whose compressed frame passes the send-buffer limit is refused.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_send_buffer_overflow', $e->getErrorCode());
		}
		self::assertSame(0, $server->getPendingOutboundLength(), 'The refused message is not queued.');

		$server->send(substr($big, 0, 400));   // repeats the refused message, which the peer never received
		self::assertSame(substr($big, 0, 400), $client->receive(), 'The next message does not refer to the refused one.');
		$server->send('after');
		self::assertSame('after', $client->receive(), 'The connection carries on compressing.');
		$a->close();
		$b->close();
	}

	// ---- The close handshake ----------------------------------------------------

	public function testDrainCloseEndsAtItsTimeoutWhileThePeerKeepsSending()
	{
		$modes = [
			'a Ping per read' => ['piece', TWebSocketFrameCodec::encode(TWebSocketFrame::ping('p'), self::KEY)],
			'a data frame per read' => ['piece', TWebSocketFrameCodec::encode(TWebSocketFrame::text('still here'), self::KEY)],
			'one long frame a byte per read' => ['trickle', TWebSocketFrameCodec::encode(TWebSocketFrame::binary(str_repeat('x', 5000)), self::KEY)],
		];
		foreach ($modes as $label => [$property, $bytes]) {
			[$transport, $peer, $clock] = $this->chattyPeer();
			$transport->$property = $bytes;
			$server = new TWebSocketConnection($transport, false);
			$server->setMonotonicClock($clock);

			self::assertFalse($server->drainClose(TWebSocketCloseCode::Normal, '', 1.0), "With $label the peer never closes, so the drain ends unanswered.");
			self::assertLessThanOrEqual(12, $transport->reads, "With $label the drain ends at its 1 s timeout (each read takes 0.1 s on the clock), however long the peer goes on.");
			self::assertGreaterThanOrEqual(9, $transport->reads, "With $label the drain reads until its timeout.");
			self::assertTrue($server->getIsClosing());
			self::assertFalse($server->getIsClosed(), 'An unanswered drain leaves the connection closing.');

			self::assertSame($transport->reads + 1, count($transport->timeouts), "With $label every read gets a read timeout, and the drain sets one more when it ends.");
			self::assertSame([1, 0], $transport->timeouts[0], 'The first read may block for the whole timeout.');
			[$seconds, $microseconds] = $transport->timeouts[5];
			self::assertSame(0, $seconds, 'A later read may block only for the time remaining.');
			self::assertEqualsWithDelta(500000, $microseconds, 1000, 'Half of the timeout remains after five reads of 0.1 s.');
			self::assertSame([1, 0], end($transport->timeouts), 'The transport is left with the full timeout as its read timeout, not the last sliver of it.');
			$transport->close();
			$peer->close();
		}
	}

	public function testDrainCloseWithoutATimeoutReadsUntilTheStreamEnds()
	{
		[$transport, $peer, $clock] = $this->chattyPeer();
		$transport->piece = TWebSocketFrameCodec::encode(TWebSocketFrame::ping('p'), self::KEY);
		$transport->limit = 40;
		$server = new TWebSocketConnection($transport, false);
		$server->setMonotonicClock($clock);
		self::assertTrue($server->drainClose(TWebSocketCloseCode::Normal, '', null), 'An unbounded drain ends when the peer ends the stream.');
		self::assertGreaterThan(40, $transport->reads, 'It reads everything the peer sent first.');
		self::assertTrue($server->getIsClosed());
		$transport->close();
	}

	public function testDrainCloseWithoutATimeoutPollsANonBlockingPeerUntilTheStreamEnds()
	{
		[$transport, $peer, $clock] = $this->chattyPeer();
		$transport->setBlocking(false);
		$transport->limit = 3;   // the peer sends nothing: three reads find no bytes, then it ends the stream
		$server = new TWebSocketConnection($transport, false);
		$start = microtime(true);
		self::assertTrue($server->drainClose(TWebSocketCloseCode::Normal, '', null), 'An unbounded drain of a silent non-blocking peer ends when the stream does.');
		self::assertSame(4, $transport->reads);
		self::assertGreaterThanOrEqual(0.1, microtime(true) - $start, 'Between empty reads the drain waits on the socket; it does not spin.');
		self::assertSame([], $transport->timeouts, 'An unbounded drain sets no read timeout.');
		$transport->close();
	}

	public function testDrainCloseEndsOnAMalformedFrameFromThePeer()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		$a->write(TWebSocketFrameCodec::encode(TWebSocketFrame::ping('p'), self::KEY) . "\x81\x05hello");   // a Ping, then an unmasked frame
		self::assertTrue($server->drainClose(TWebSocketCloseCode::Normal, '', 0.5), 'A protocol error from the peer ends the drain: the connection is closed regardless.');
		self::assertTrue($server->getIsClosed());
		self::assertTrue($server->getIsFailed());
		$a->close();
		$b->close();
	}

	public function testDrainCloseLeavesABlockingTransportWithTheFullTimeout()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$start = microtime(true);
		self::assertFalse($client->drainClose(TWebSocketCloseCode::Normal, '', 0.1), 'A silent blocking peer does not answer.');
		self::assertGreaterThanOrEqual(0.08, microtime(true) - $start, 'The blocking read waits out the timeout.');

		$start = microtime(true);
		self::assertNull($client->receiveFrame(), 'A later read times out too.');
		self::assertTrue($client->getReadTimedOut());
		self::assertGreaterThanOrEqual(0.08, microtime(true) - $start, 'The transport keeps the drain\'s full timeout as its read timeout, not the last sliver of it.');
		$a->close();
		$b->close();
	}

	public function testDrainCloseLetsAnUnsendableCodeOut()
	{
		[$a, $b] = TSocketStream::pair();
		$b->setBlocking(false);
		$server = new TWebSocketConnection($a, false);
		foreach ([[TWebSocketCloseCode::NoStatusReceived, '', 'websocket_close_code_not_sendable'], [TWebSocketCloseCode::Normal, str_repeat('r', 124), 'websocket_close_reason_too_long']] as [$code, $reason, $error]) {
			try {
				$server->drainClose($code, $reason, 0.05);
				self::fail('A Close that cannot be built is the caller\'s error.');
			} catch (TInvalidDataValueException $e) {
				self::assertSame($error, $e->getErrorCode());
			}
			self::assertFalse($server->getIsClosing(), 'Nothing was sent, so the connection is not closing.');
			self::assertFalse($server->getIsClosed(), 'A refused drain does not report the connection closed.');
		}
		self::assertSame('', $b->read(100), 'No byte reached the wire.');

		$b->write(TWebSocketFrameCodec::encode(TWebSocketFrame::close(TWebSocketCloseCode::Normal), self::KEY));
		self::assertTrue($server->drainClose(TWebSocketCloseCode::Normal, 'bye', 0.5), 'The connection is still usable: a valid drain completes.');
		self::assertTrue($server->drainClose(TWebSocketCloseCode::NoStatusReceived), 'Once closing, the code is not used and so not checked.');
		$a->close();
		$b->close();
	}

	public function testDrainCloseTreatsAnUnwritableCloseAsClosed()
	{
		[$client, $server, $a, $b] = $this->pair();
		$a->close();   // the peer is gone: the Close cannot be written
		set_error_handler([Prado::class, 'phpErrorHandler']);
		try {
			self::assertTrue($server->drainClose(TWebSocketCloseCode::Normal, '', 0.05), 'A Close that cannot be written ends the drain: the connection is closed.');
		} finally {
			restore_error_handler();
		}
		self::assertTrue($server->getIsClosed());
		$b->close();
	}

	public function testDrainCloseOnAFailedConnectionSendsTheCloseAndReadsNothing()
	{
		[$transport, $peer, $clock] = $this->chattyPeer();
		$transport->piece = TWebSocketFrameCodec::encode(TWebSocketFrame::text('more'), self::KEY);
		$server = new TWebSocketConnection($transport, false);
		$peer->write("\x81\x05hello");   // an unmasked frame
		try {
			$server->receive();
			self::fail('An unmasked frame is a protocol error.');
		} catch (TWebSocketException $e) {
			$code = $e->getCloseCode();
		}
		$reads = $transport->reads;
		self::assertTrue($server->drainClose($code, '', 0.3), 'Failing the connection ends it: no Close is awaited from a peer whose input is not parsed.');
		self::assertSame($reads, $transport->reads, 'Nothing more is read from a failed connection.');
		self::assertTrue($server->getIsClosed());
		$frames = self::serverFrames($peer);
		self::assertSame(TWebSocketCloseCode::ProtocolError, end($frames)->getCloseCode(), 'The Close naming the failure was sent.');
		$transport->close();
		$peer->close();
	}

	// ---- handleConnection: the closing phase -------------------------------------

	public function testHandleConnectionStopsWaitingForThePeersCloseAtTheCloseTimeout()
	{
		[$transport, $peer, $clock] = $this->chattyPeer();
		$transport->piece = TWebSocketFrameCodec::encode(TWebSocketFrame::text('still here'), self::KEY);
		$server = new TWebSocketConnection($transport, false);
		$server->setMonotonicClock($clock);
		$handler = new TWebSocketHandler();
		self::assertSame(5.0, $handler->getCloseTimeout(), 'The close timeout defaults to 5 seconds.');
		$handler->setCloseTimeout('1');
		self::assertSame(1.0, $handler->getCloseTimeout());
		$events = [];
		$handler->attachEventHandler('onOpen', function ($connection) use (&$events) {
			$events[] = 'open';
			$connection->close(TWebSocketCloseCode::PolicyViolation, 'refused');
		});
		$handler->attachEventHandler('onMessage', function () use (&$events) {
			$events[] = 'message';
		});
		$handler->attachEventHandler('onError', function () use (&$events) {
			$events[] = 'error';
		});
		$handler->attachEventHandler('onClose', function () use (&$events) {
			$events[] = 'close';
		});

		$handler->handleConnection($server);

		self::assertSame(['open', 'close'], $events, 'Nothing the peer sends after the application closed is dispatched.');
		self::assertLessThanOrEqual(12, $transport->reads, 'The loop returns at the 1 s close timeout (each read takes 0.1 s on the clock), though the peer keeps sending.');
		self::assertTrue($server->getIsClosing());
		self::assertFalse($server->getIsClosed(), 'The peer never sent its Close.');
		$transport->close();
		$peer->close();
	}

	public function testHandleConnectionWithoutACloseTimeoutReadsOnToTheEndOfTheStream()
	{
		[$transport, $peer, $clock] = $this->chattyPeer();
		$transport->piece = TWebSocketFrameCodec::encode(TWebSocketFrame::text('still here'), self::KEY);
		$transport->limit = 60;
		$server = new TWebSocketConnection($transport, false);
		$server->setMonotonicClock($clock);
		$handler = new TWebSocketHandler();
		$handler->setCloseTimeout(-3);
		self::assertSame(0.0, $handler->getCloseTimeout(), 'A negative close timeout is no bound.');
		$dispatched = 0;
		$handler->attachEventHandler('onOpen', fn ($connection) => $connection->close());
		$handler->attachEventHandler('onMessage', function () use (&$dispatched) {
			$dispatched++;
		});
		$handler->handleConnection($server);
		self::assertSame(0, $dispatched);
		self::assertGreaterThan(60, $transport->reads, 'With no bound the loop reads until the peer ends the stream.');
		self::assertTrue($server->getIsClosed());
		$transport->close();
	}

	public function testHandleConnectionDispatchesNothingAfterTheApplicationClosesInOnMessage()
	{
		[$client, $server, $a, $b] = $this->pair();
		$client->send('first');
		$client->send('second');
		$client->close(TWebSocketCloseCode::Normal);
		$handler = new TWebSocketHandler();
		$seen = [];
		$handler->attachEventHandler('onMessage', function ($connection, $message) use (&$seen) {
			$seen[] = (string) $message;
			$connection->close(TWebSocketCloseCode::Normal, 'done');
		});
		$handler->handleConnection($server);
		self::assertSame(['first'], $seen, 'The message behind the application\'s close is read and dropped.');
		self::assertTrue($server->getIsClosed(), 'The peer\'s Close completes the handshake within the close timeout.');
		$a->close();
		$b->close();
	}

	public function testHandleConnectionBoundsACloseMadeFromAConnectionEvent()
	{
		// The application closes from onPing, inside receiveMessage(): the message behind the Ping is not dispatched.
		[$client, $server, $a, $b] = $this->pair();
		$client->ping('now');
		$client->send('late');
		$client->close(TWebSocketCloseCode::Normal);
		$server->attachEventHandler('onPing', fn () => $server->close(TWebSocketCloseCode::Normal));
		$handler = new TWebSocketHandler();
		$seen = [];
		$handler->attachEventHandler('onMessage', function ($connection, $message) use (&$seen) {
			$seen[] = (string) $message;
		});
		$handler->handleConnection($server);
		self::assertSame([], $seen);
		self::assertTrue($server->getIsClosed());
		$a->close();
		$b->close();

		// The same close, with a peer that then stays silent: the read times out and the close timeout ends the wait.
		[$client, $server, $a, $b] = $this->pair();
		$b->setTimeout(0, 20000);
		$client->ping('now');
		$server->attachEventHandler('onPing', fn () => $server->close(TWebSocketCloseCode::Normal));
		$handler = new TWebSocketHandler();
		$handler->setCloseTimeout(0.05);
		$start = microtime(true);
		$handler->handleConnection($server);
		self::assertGreaterThanOrEqual(0.04, microtime(true) - $start, 'The loop waits the close timeout for the peer\'s Close.');
		self::assertTrue($server->getIsClosing());
		self::assertFalse($server->getIsClosed());
		$frames = self::serverFrames($a);
		self::assertCount(1, $frames, 'Only the Close was written: no keepalive Ping follows it.');
		self::assertSame(TWebSocketOpcode::Close, $frames[0]->getOpcode());
		$a->close();
		$b->close();
	}

	public function testHandleConnectionFailsTheConnectionOnAnError()
	{
		// A protocol error from the peer: onError, then a Close naming it.
		[$client, $server, $a, $b] = $this->pair();
		$a->write("\x81\x05hello");   // an unmasked frame
		$handler = new TWebSocketHandler();
		$events = [];
		$handler->attachEventHandler('onError', function ($connection, $error) use (&$events) {
			$events[] = $error instanceof TWebSocketException ? $error->getErrorCode() : get_class($error);
		});
		$handler->attachEventHandler('onClose', function () use (&$events) {
			$events[] = 'close';
		});
		$handler->handleConnection($server);
		self::assertSame(['websocket_frame_not_masked', 'close'], $events);
		self::assertTrue($server->getIsFailed());
		$frames = self::serverFrames($a);
		self::assertCount(1, $frames);
		self::assertSame(TWebSocketCloseCode::ProtocolError, $frames[0]->getCloseCode(), 'The Close carries the error\'s close code.');
		$a->close();
		$b->close();

		// An exception from the application: onError, then a Close with 1011.
		[$client, $server, $a, $b] = $this->pair();
		$client->send('boom');
		$handler = new TWebSocketHandler();
		$events = [];
		$handler->attachEventHandler('onMessage', function () {
			throw new \RuntimeException('handler bug');
		});
		$handler->attachEventHandler('onError', function ($connection, $error) use (&$events) {
			$events[] = $error->getMessage();
		});
		$handler->handleConnection($server);
		self::assertSame(['handler bug'], $events);
		$frames = self::serverFrames($a);
		self::assertSame(TWebSocketCloseCode::InternalServerError, end($frames)->getCloseCode());
		$a->close();
		$b->close();

		// The application closes and then throws: the error is raised, and its own Close stays the one sent.
		[$client, $server, $a, $b] = $this->pair();
		$client->send('boom');
		$handler = new TWebSocketHandler();
		$errors = 0;
		$handler->attachEventHandler('onMessage', function ($connection) {
			$connection->close(TWebSocketCloseCode::PolicyViolation);
			throw new \RuntimeException('after close');
		});
		$handler->attachEventHandler('onError', function () use (&$errors) {
			$errors++;
		});
		$handler->handleConnection($server);
		self::assertSame(1, $errors);
		$frames = self::serverFrames($a);
		self::assertCount(1, $frames, 'No second Close follows the application\'s.');
		self::assertSame(TWebSocketCloseCode::PolicyViolation, $frames[0]->getCloseCode());
		$a->close();
		$b->close();
	}

	public function testHandleConnectionStillClosesWhenTheCloseCannotBeWritten()
	{
		[$client, $server, $a, $b] = $this->pair();
		$client->send('boom');
		$handler = new TWebSocketHandler();
		$events = [];
		$handler->attachEventHandler('onMessage', function () use ($a) {
			$a->close();   // the peer goes away, then the application fails
			throw new \RuntimeException('handler bug');
		});
		$handler->attachEventHandler('onError', function () use (&$events) {
			$events[] = 'error';
		});
		$handler->attachEventHandler('onClose', function () use (&$events) {
			$events[] = 'close';
		});
		set_error_handler([Prado::class, 'phpErrorHandler']);
		try {
			$handler->handleConnection($server);
		} finally {
			restore_error_handler();
		}
		self::assertSame(['error', 'close'], $events, 'A Close that cannot be written is logged, and onClose still runs.');
		self::assertTrue($server->getIsClosed(), 'The failed write closed the connection.');
		$b->close();
	}

	// ---- wss:// ------------------------------------------------------------------

	/**
	 * Writes a self-signed certificate and key for a host name to a temporary PEM file.
	 * @param string $commonName The certificate's common name.
	 * @return string The PEM file path.
	 */
	private function selfSignedCertificate(string $commonName): string
	{
		$key = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
		$csr = openssl_csr_new(['commonName' => $commonName], $key, ['digest_alg' => 'sha256']);
		$cert = openssl_csr_sign($csr, null, $key, 1, ['digest_alg' => 'sha256']);
		openssl_x509_export($cert, $certPem);
		openssl_pkey_export($key, $keyPem);
		$file = tempnam(sys_get_temp_dir(), 'wstls');
		file_put_contents($file, $certPem . $keyPem);
		$this->tempFiles[] = $file;
		return $file;
	}

	/**
	 * Dials a `wss://` URL on a TLS listener in this process and runs both TLS handshakes to their end.
	 * @param resource $listener The listening socket, whose context holds the server certificate.
	 * @param string $url The `wss://` URL to dial.
	 * @param array<string, mixed> $ssl The client's `ssl` context options on top of peer verification.
	 * @return array{0: AnsweredClientStream, 1: TSocketStream, 2: bool} The client transport, the
	 *   server's end, and whether the client verified the server and completed the TLS handshake.
	 */
	private function dialTls($listener, string $url, array $ssl): array
	{
		$parts = TWebSocketHandshake::parseUrl($url);
		self::assertTrue($parts['secure'], 'A wss:// URL asks for TLS.');
		$context = stream_context_create(['ssl' => $ssl + ['verify_peer' => true, 'verify_peer_name' => true, 'peer_name' => $parts['host']]]);
		$raw = stream_socket_client('tcp://' . $parts['host'] . ':' . $parts['port'], $errno, $errstr, 2.0, STREAM_CLIENT_CONNECT, $context);
		self::assertIsResource($raw, "The TCP connect failed: $errstr");
		$accepted = stream_socket_accept($listener, 2.0);
		self::assertIsResource($accepted);
		$client = new AnsweredClientStream();
		$client->attachResource($raw, true);
		$server = new TSocketStream();
		$server->attachResource($accepted, true);
		$client->setBlocking(false);
		$server->setBlocking(false);
		$clientCrypto = 0;
		$serverCrypto = 0;
		for ($i = 0; $i < 2000 && $clientCrypto !== false && $serverCrypto !== false && ($clientCrypto !== true || $serverCrypto !== true); $i++) {
			// Both ends are in this process, so their handshakes advance in turn; a failed step warns, and its false is the result.
			if ($serverCrypto !== true) {
				$serverCrypto = @$server->enableCrypto(true, STREAM_CRYPTO_METHOD_TLS_SERVER);
			}
			if ($clientCrypto !== true) {
				$clientCrypto = @$client->enableCrypto(true, STREAM_CRYPTO_METHOD_TLS_CLIENT);
			}
			if ($clientCrypto === 0 && $serverCrypto === 0) {
				usleep(1000);
			}
		}
		$client->setBlocking(true);
		$server->setBlocking(true);
		$client->setTimeout(10);
		$server->setTimeout(10);
		return [$client, $server, $clientCrypto === true && $serverCrypto === true];
	}

	public function testClientDialsWssWithCertificateVerification()
	{
		if (!extension_loaded('openssl')) {
			$this->markTestSkipped('ext-openssl is not available.');
		}
		$certificate = $this->selfSignedCertificate('127.0.0.1');
		$listener = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, stream_context_create(['ssl' => ['local_cert' => $certificate]]));
		self::assertIsResource($listener, "The listener failed to bind: $errstr");
		$port = (int) substr(strrchr(stream_socket_get_name($listener, false), ':'), 1);
		$url = "wss://127.0.0.1:$port/chat?room=1";

		[$client, $serverEnd, $secure] = $this->dialTls($listener, $url, ['cafile' => $certificate]);
		self::assertTrue($secure, 'The client verifies the certificate against its CA file and the URL host, and the TLS handshake completes.');
		self::assertNotSame([], $client->getCryptoMeta(), 'The transport is encrypted.');

		$request = null;
		$server = null;
		$client->answer = function () use ($serverEnd, &$request, &$server) {
			$request = TWebSocketHandshake::acceptConnection($serverEnd, ['subprotocols' => ['chat'], 'timeout' => 10.0]);
			$server = new TWebSocketConnection($serverEnd, false);
		};
		$connection = TWebSocketConnection::connectUrl($client, $url, ['subprotocols' => ['chat'], 'timeout' => 10.0]);

		self::assertTrue($connection->getIsClient());
		self::assertSame('chat', $connection->getSubprotocol(), 'The subprotocol is negotiated over TLS.');
		self::assertSame('/chat?room=1', $request['target'], 'The request target comes from the URL.');
		self::assertSame("127.0.0.1:$port", $request['headers']['host'], 'The Host header names the URL authority.');

		$connection->send('secure');
		self::assertSame('secure', $server->receive(), 'A masked client frame crosses the encrypted transport.');
		$server->send('echo:secure');
		self::assertSame('echo:secure', $connection->receive());

		$connection->close(TWebSocketCloseCode::Normal, 'done');
		self::assertNull($server->receive(), 'The server reads the client\'s Close and echoes it.');
		self::assertTrue($server->getIsClosed());
		self::assertTrue($connection->drainClose(), 'The client completes the close handshake over TLS.');
		$client->close();
		$serverEnd->close();
		fclose($listener);
	}

	public function testWssDialRefusesAnUntrustedOrMisnamedCertificate()
	{
		if (!extension_loaded('openssl')) {
			$this->markTestSkipped('ext-openssl is not available.');
		}
		$certificate = $this->selfSignedCertificate('127.0.0.1');
		$other = $this->selfSignedCertificate('127.0.0.1');
		$listener = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, stream_context_create(['ssl' => ['local_cert' => $certificate]]));
		self::assertIsResource($listener, "The listener failed to bind: $errstr");
		$port = (int) substr(strrchr(stream_socket_get_name($listener, false), ':'), 1);
		$cases = [
			'A certificate no CA of the client signed' => ['cafile' => $other],
			'A certificate for another host name' => ['cafile' => $certificate, 'peer_name' => 'other.example'],
		];
		foreach ($cases as $label => $ssl) {
			[$client, $serverEnd, $secure] = $this->dialTls($listener, "wss://127.0.0.1:$port/", $ssl);
			self::assertFalse($secure, "$label fails the TLS handshake, so no WebSocket handshake is sent.");
			$client->close();
			$serverEnd->close();
		}
		fclose($listener);
	}
}
