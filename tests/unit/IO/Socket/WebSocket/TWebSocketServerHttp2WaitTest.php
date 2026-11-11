<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\IO\Async\TAsync;
use Prado\IO\Http2\TH2Session;
use Prado\IO\Http2\TNgHttp2;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketServer;

/**
 * The server's per-tick housekeeping during a handler's wait on the main fiber, over HTTP/2, where
 * a stream's output reaches the transport only when the housekeeping pulls it from the session.
 */
class TWebSocketServerHttp2WaitTest extends TestCase
{
	protected function tearDown(): void
	{
		TAsync::setLoop(null);
	}

	public function testHttp2OutputSentBeforeAMainFiberWaitReachesThePeerDuringTheWait()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$reactor = $server->getReactor();
		TAsync::setLoop($reactor);   // as serve() does
		$socket = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$stream = $client->request([':method' => 'CONNECT', ':protocol' => 'websocket', ':scheme' => 'https', ':path' => '/', ':authority' => 'h', 'sec-websocket-version' => '13']);
		$clientWs = new TWebSocketConnection($stream, true);
		$received = [];
		$read = function () use ($socket, $client, $stream, $clientWs, &$received): void {
			$bytes = $socket->read(65536);
			if ($bytes !== '') {
				$client->receive($bytes);
			}
			foreach ($clientWs->feed($stream->getContents()) as $message) {
				$received[] = $message;
			}
		};
		$handler = new TWebSocketHandler();
		$duringWait = null;
		$handler->attachEventHandler('onMessage', function ($conn) use ($reactor, $read, &$received, &$duringWait) {
			$conn->send('progress');   // queued in the HTTP/2 stream before the wait
			$watch = $reactor->repeat(0.02, $read);   // the peer reads throughout the wait
			TAsync::sleep(0.3);
			$reactor->cancel($watch);
			$duringWait = $received;
			$conn->send('done');
		});
		$server->setHandler($handler);

		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('go'), random_bytes(4)));
		$socket->write($client->send());
		$socket->setBlocking(false);
		for ($i = 0; $i < 100 && count($received) < 2; $i++) {
			$server->serveOnce(0, 20000);
			if ($duringWait !== null) {
				$read();
			}
		}

		self::assertSame(['progress'], $duringWait, 'Output a handler sent before it waits on the main fiber is pulled from the HTTP/2 session and written during the wait.');
		self::assertSame(['progress', 'done'], $received, 'The output sent after the wait follows.');
		$socket->close();
		$server->close();
	}
}
