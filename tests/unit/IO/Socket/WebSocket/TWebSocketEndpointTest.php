<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\IWebSocketEndpoint;
use Prado\IO\Socket\WebSocket\IWebSocketEndpointLimits;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\Socket\WebSocket\TWebSocketServer;

/** A stand-in internal endpoint (e.g. an /admin channel) recording how the server dispatches to it. */
class RecordingEndpoint implements IWebSocketEndpoint
{
	public int $authCalls = 0;
	public ?TWebSocketConnection $accepted = null;
	public ?TSocketStream $transport = null;
	/** @var array<string, mixed> */
	public array $request = [];

	public function __construct(private string $path = '/admin', private bool $authorized = true)
	{
	}

	public function matchesTarget(?string $target): bool
	{
		return $target === $this->path;
	}

	public function authenticate(array $headers): bool
	{
		$this->authCalls++;
		return $this->authorized;
	}

	public function accept(TWebSocketConnection $connection, TSocketStream $transport, array $request): void
	{
		$this->accepted = $connection;
		$this->transport = $transport;
		$this->request = $request;
	}
}

/** An endpoint that declares its own connection limits, as a mesh peer link does. */
class LimitedEndpoint extends RecordingEndpoint implements IWebSocketEndpointLimits
{
	public function __construct(string $path, private ?int $maxMessageSize, private ?int $maxSendBufferBytes)
	{
		parent::__construct($path);
	}

	public function getEndpointMaxMessageSize(): ?int
	{
		return $this->maxMessageSize;
	}

	public function getEndpointMaxSendBufferBytes(): ?int
	{
		return $this->maxSendBufferBytes;
	}
}

class TWebSocketEndpointTest extends TestCase
{
	private function upgrade(TWebSocketServer $server, string $path): TSocketStream
	{
		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('host', $path, TWebSocketHandshake::generateKey()));
		return $client;
	}

	public function testCustomEndpointIsMatchedAuthenticatedAndAccepted()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$server->setMaxMessageSize(4096);
		$server->setMaxSendBufferBytes(8192);
		$endpoint = new RecordingEndpoint('/admin');
		$server->addEndpoint($endpoint);

		$opened = 0;
		$server->attachEventHandler('onConnection', function () use (&$opened): void {
			$opened++;
		});

		$client = $this->upgrade($server, '/admin');
		for ($tick = 0; $tick < 5; $tick++) {
			$server->serveOnce(0, 60000);   // the accept, the request and the reply can take separate ticks
		}

		self::assertSame(1, $endpoint->authCalls, 'The endpoint authenticated the upgrade.');
		self::assertInstanceOf(TWebSocketConnection::class, $endpoint->accepted, 'The endpoint took over the upgraded connection.');
		self::assertSame(0, $opened, 'An internal endpoint upgrade is not a normal client.');
		self::assertSame(4096, $endpoint->accepted->getMaxMessageSize(), 'The server MaxMessageSize applies to an endpoint connection.');
		self::assertSame(8192, $endpoint->accepted->getMaxSendBufferBytes(), 'The server MaxSendBufferBytes applies to an endpoint connection.');
		self::assertFalse($endpoint->accepted->getIsClient());
		self::assertSame('/admin', $endpoint->request['target'], 'The endpoint receives the parsed request.');
		self::assertSame('host', $endpoint->request['headers']['host']);
		self::assertInstanceOf(TSocketStream::class, $endpoint->transport);
		self::assertFalse($endpoint->transport->getBlocking(), 'The endpoint transport is non-blocking.');
		self::assertStringContainsString('101 Switching Protocols', $client->read(4096), 'The server wrote the 101 before handing over.');

		$client->close();
		$server->close();
	}

	public function testAnEndpointDeclaresItsOwnConnectionLimits()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$server->setMaxMessageSize(1000);   // a small client limit
		$server->setMaxSendBufferBytes(2000);
		$endpoint = new LimitedEndpoint('/cluster', TWebSocketConnection::DEFAULT_MAX_MESSAGE_SIZE, 0);
		$server->addEndpoint($endpoint);

		$client = $this->upgrade($server, '/cluster');
		for ($tick = 0; $tick < 5 && $endpoint->accepted === null; $tick++) {
			$server->serveOnce(0, 60000);
		}
		self::assertInstanceOf(TWebSocketConnection::class, $endpoint->accepted);
		self::assertSame(TWebSocketConnection::DEFAULT_MAX_MESSAGE_SIZE, $endpoint->accepted->getMaxMessageSize(), 'The endpoint\'s declared limit replaces the client MaxMessageSize.');
		self::assertSame(0, $endpoint->accepted->getMaxSendBufferBytes(), 'A declared 0 is unlimited.');
		$client->read(4096);   // the 101
		$payload = str_repeat('e', 1500);   // a relayed envelope larger than the client limit
		(new TWebSocketConnection($client, true))->sendBinary($payload);
		$received = [];
		for ($i = 0; $i < 100 && $received === []; $i++) {
			$read = [$endpoint->transport->getResource()];
			$write = $except = null;
			stream_select($read, $write, $except, 0, 20000);
			foreach ($endpoint->accepted->feedMessages($endpoint->transport->read(65536)) as $message) {
				$received[] = $message->getPayload();
			}
		}
		self::assertSame([$payload], $received, 'The endpoint link carries a message past the client limit.');

		$client->close();
		$server->close();
	}

	public function testAnEndpointThatDeclaresNoLimitKeepsTheServers()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$server->setMaxMessageSize(1000);
		$server->setMaxSendBufferBytes(2000);
		$endpoint = new LimitedEndpoint('/admin', null, null);
		$server->addEndpoint($endpoint);

		$client = $this->upgrade($server, '/admin');
		for ($tick = 0; $tick < 5 && $endpoint->accepted === null; $tick++) {
			$server->serveOnce(0, 60000);
		}
		self::assertSame(1000, $endpoint->accepted->getMaxMessageSize(), 'A null limit keeps the server MaxMessageSize.');
		self::assertSame(2000, $endpoint->accepted->getMaxSendBufferBytes(), 'A null limit keeps the server MaxSendBufferBytes.');
		$client->close();
		$server->close();
	}

	public function testCustomEndpointRejectsWhenAuthenticationFailsWith403()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$endpoint = new RecordingEndpoint('/admin', authorized: false);
		$server->addEndpoint($endpoint);

		$client = $this->upgrade($server, '/admin');
		for ($tick = 0; $tick < 5; $tick++) {
			$server->serveOnce(0, 60000);   // the accept, the request and the reply can take separate ticks
		}

		self::assertSame(1, $endpoint->authCalls);
		self::assertNull($endpoint->accepted, 'A rejected upgrade is refused, not accepted.');
		$response = $client->read(4096);
		self::assertStringStartsWith('HTTP/1.1 403', $response, 'The refusal is written as a 403 before the transport closes.');
		self::assertSame('', $client->read(16), 'The transport is then closed.');
		self::assertSame(0, $server->getConnectionCount());

		$client->close();
		$server->close();
	}

	public function testANonMatchingPathFallsThroughToTheClientPath()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$cluster = new TWebSocketCluster('n1', new TNullBackplane());
		$server->setCluster($cluster);
		$server->setHandler(new TWebSocketHandler());
		$endpoint = new RecordingEndpoint('/admin');
		$server->addEndpoint($endpoint);

		$client = $this->upgrade($server, '/chat');
		for ($tick = 0; $tick < 5; $tick++) {
			$server->serveOnce(0, 60000);   // the accept, the request and the reply can take separate ticks
		}

		self::assertNull($endpoint->accepted, 'A non-matching path does not reach the endpoint.');
		self::assertCount(1, $cluster->presence(), 'A non-matching path is a normal client.');

		$client->close();
		$server->close();
	}

	public function testEndpointLinksCountTowardMaxConnections()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$server->setMaxConnections(1);
		$endpoint = new RecordingEndpoint('/admin');
		$server->addEndpoint($endpoint);

		$link = $this->upgrade($server, '/admin');
		for ($tick = 0; $tick < 5; $tick++) {
			$server->serveOnce(0, 60000);   // the accept, the request and the reply can take separate ticks
		}
		self::assertInstanceOf(TWebSocketConnection::class, $endpoint->accepted);
		self::assertSame(1, $server->getLoad(), 'A connection an endpoint took over is still load.');

		$second = $this->upgrade($server, '/admin');
		for ($tick = 0; $tick < 5; $tick++) {
			$server->serveOnce(0, 60000);   // the accept, the request and the reply can take separate ticks
		}
		self::assertStringContainsString('503', $second->read(4096), 'A further endpoint upgrade is shed at the cap.');

		$endpoint->transport->close();   // the endpoint releases its link
		self::assertSame(0, $server->getLoad(), 'A closed endpoint link leaves the load.');
		$third = $this->upgrade($server, '/chat');
		for ($tick = 0; $tick < 5; $tick++) {
			$server->serveOnce(0, 60000);   // the accept, the request and the reply can take separate ticks
		}
		self::assertStringContainsString('101', $third->read(4096), 'The freed slot admits a client.');

		$link->close();
		$second->close();
		$third->close();
		$server->close();
	}

	public function testMeshBackplaneIsExposedAsAnEndpoint()
	{
		$server = new TWebSocketServer();
		$mesh = new TMeshBackplane();
		$server->setCluster(new TWebSocketCluster('n1', $mesh));

		self::assertContains($mesh, $server->getEndpoints(), 'The mesh backplane is an internal endpoint.');
		self::assertFalse($mesh->matchesTarget('/cluster'), 'Without a secret the mesh is disabled and claims no path.');
		$mesh->setSecret('shh');
		self::assertTrue($mesh->matchesTarget('/cluster'), 'With a secret the mesh claims its peer path.');
		self::assertFalse($mesh->matchesTarget('/elsewhere'));
	}

	public function testAddedEndpointsPrecedeTheBackplane()
	{
		$server = new TWebSocketServer();
		$mesh = new TMeshBackplane();
		$server->setCluster(new TWebSocketCluster('n1', $mesh));
		$first = new RecordingEndpoint('/admin');
		$server->addEndpoint($first);

		self::assertSame([$first, $mesh], $server->getEndpoints(), 'Added endpoints match before the mesh.');
	}

	public function testSetEndpointsRejectsANonEndpoint()
	{
		$server = new TWebSocketServer();
		try {
			$server->setEndpoints([new \stdClass()]);
			self::fail('An entry that is not an endpoint is refused.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_endpoint_invalid', $e->getErrorCode());
		}
	}
}
