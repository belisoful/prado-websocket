<?php

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\IWebSocketEndpoint;
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
	}
}

class TWebSocketEndpointTest extends PHPUnit\Framework\TestCase
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
		$endpoint = new RecordingEndpoint('/admin');
		$server->addEndpoint($endpoint);

		$opened = 0;
		$server->attachEventHandler('onConnection', function () use (&$opened): void {
			$opened++;
		});

		$client = $this->upgrade($server, '/admin');
		$server->serveOnce(0, 300000);

		self::assertSame(1, $endpoint->authCalls, 'The endpoint authenticated the upgrade.');
		self::assertInstanceOf(TWebSocketConnection::class, $endpoint->accepted, 'The endpoint took over the upgraded connection.');
		self::assertSame(0, $opened, 'An internal endpoint upgrade is not a normal client.');

		$client->close();
		$server->close();
	}

	public function testCustomEndpointRejectsWhenAuthenticationFails()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$endpoint = new RecordingEndpoint('/admin', authorized: false);
		$server->addEndpoint($endpoint);

		$client = $this->upgrade($server, '/admin');
		$server->serveOnce(0, 300000);

		self::assertSame(1, $endpoint->authCalls);
		self::assertNull($endpoint->accepted, 'A rejected upgrade is refused, not accepted.');

		$client->close();
		$server->close();
	}

	public function testANonMatchingPathFallsThroughToTheClientPath()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$cluster = new TWebSocketCluster('n1', new \Prado\IO\Socket\WebSocket\Cluster\TNullBackplane());
		$server->setCluster($cluster);
		$server->setHandler(new TWebSocketHandler());
		$endpoint = new RecordingEndpoint('/admin');
		$server->addEndpoint($endpoint);

		$client = $this->upgrade($server, '/chat');
		$server->serveOnce(0, 300000);

		self::assertNull($endpoint->accepted, 'A non-matching path does not reach the endpoint.');
		self::assertCount(1, $cluster->presence(), 'A non-matching path is a normal client.');

		$client->close();
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
		$this->expectException(TWebSocketException::class);
		$server->setEndpoints([new \stdClass()]);
	}
}
