<?php

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\Web\Services\TWebSocketService;

class TWebSocketServiceTest extends PHPUnit\Framework\TestCase
{
	/** @return array{0: TWebSocketConnection, 1: TWebSocketConnection, 2: TSocketStream, 3: TSocketStream} */
	private function pair(): array
	{
		[$a, $b] = TSocketStream::pair();
		return [new TWebSocketConnection($a, true), new TWebSocketConnection($b, false), $a, $b];
	}

	public function testHandleConnectionRaisesOpenMessageClose()
	{
		[$client, $server, $a, $b] = $this->pair();
		$client->send('one');
		$client->send('two');
		$client->close(1000);

		$service = new TWebSocketService();
		$events = [];
		$service->attachEventHandler('onOpen', function ($conn) use (&$events) {
			$events[] = ['open', $conn];
		});
		$service->attachEventHandler('onMessage', function ($conn, $msg) use (&$events) {
			$events[] = ['message', $msg];
		});
		$service->attachEventHandler('onClose', function ($conn) use (&$events) {
			$events[] = ['close', $conn];
		});

		$service->handleConnection($server);

		self::assertSame('open', $events[0][0]);
		self::assertSame($server, $events[0][1]);
		self::assertSame(['message', 'one'], $events[1]);
		self::assertSame(['message', 'two'], $events[2]);
		self::assertSame('close', $events[3][0]);
		self::assertTrue($server->getIsClosed());
		$a->close();
		$b->close();
	}

	public function testRunHandlesInjectedConnection()
	{
		[$client, $server, $a, $b] = $this->pair();
		$client->send('hi');
		$client->close(1000);

		$service = new TWebSocketService();
		$service->setConnection($server);
		self::assertSame($server, $service->getConnection());

		$messages = [];
		$service->attachEventHandler('onMessage', function ($conn, $msg) use (&$messages) {
			$messages[] = $msg;
		});
		$service->run();

		self::assertSame(['hi'], $messages);
		$a->close();
		$b->close();
	}

	public function testRunWithoutConnectionIsNoOp()
	{
		$service = new TWebSocketService();
		$service->run();
		self::assertNull($service->getConnection());
	}

	public function testProtocolErrorRaisesErrorAndCloses()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		// A control frame with a payload over 125 bytes is a protocol error on decode.
		$a->write("\x89\x7e\x00\x80" . str_repeat('x', 128));

		$service = new TWebSocketService();
		$error = null;
		$service->attachEventHandler('onError', function ($conn, $e) use (&$error) {
			$error = $e;
		});
		$service->handleConnection($server);

		self::assertInstanceOf(\Prado\IO\Socket\WebSocket\TWebSocketException::class, $error);
		self::assertTrue($server->getIsClosing() || $server->getIsClosed());
		$a->close();
		$b->close();
	}
}
