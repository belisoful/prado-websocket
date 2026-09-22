<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\Prado;
use Prado\Util\Log\TLogger;

/** A backplane that counts how often the serve loop pumps it. */
class CountingBackplane extends TNullBackplane
{
	public int $ticks = 0;

	public function tick(): void
	{
		$this->ticks++;
	}
}

/** A backplane whose presence drop fails, as a backplane whose store is down would. */
class FailingDropBackplane extends TNullBackplane
{
	public int $drops = 0;

	public function dropPresence(string $clientId): void
	{
		$this->drops++;
		throw new \RuntimeException('backplane store is down');
	}
}

class TWebSocketServerClusterTest extends TestCase
{
	public function testAThrowingBackplaneUnregisterDoesNotEscapeServeOnce()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$backplane = new FailingDropBackplane();
		$cluster = new TWebSocketCluster('s1', $backplane);
		$server->setCluster($cluster);
		$closed = 0;
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onClose', function () use (&$closed): void {
			$closed++;
		});
		$server->setHandler($handler);
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 300000);
		self::assertCount(1, $cluster->presence());
		self::assertStringContainsString('101', $client->read(4096));

		(new TWebSocketConnection($client, true))->close(1000);
		$server->serveOnce(0, 300000);   // the unregister throws inside the backplane; the loop must survive it

		self::assertSame(1, $backplane->drops, 'The backplane was asked to drop the presence.');
		self::assertSame(1, $closed, 'The handler close ran.');
		self::assertCount(0, $cluster->presence(), 'The local presence is gone although the backplane failed.');
		self::assertSame(0, $server->getConnectionCount(), 'The transport is closed although the backplane failed.');
		$client->read(16);   // the echoed Close
		self::assertSame('', $client->read(16), 'The peer sees end of stream.');
		$logs = Prado::getLogger()->getLogs(TLogger::WARNING, TWebSocketServer::class);
		self::assertNotEmpty($logs, 'The swallowed backplane fault is logged.');
		self::assertStringContainsString('backplane store is down', $logs[0][TLogger::LOG_MESSAGE]);
		$server->serveOnce(0, 20000);   // the loop keeps serving
		$client->close();
		$server->close();
	}

	public function testClusterIsSettable()
	{
		$server = new TWebSocketServer();
		self::assertNull($server->getCluster(), 'A server is standalone by default.');
		$cluster = new TWebSocketCluster('s1', new TNullBackplane());
		$server->setCluster($cluster);
		self::assertSame($cluster, $server->getCluster());
	}

	public function testServeLoopRegistersPumpsAndUnregisters()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$backplane = new CountingBackplane();
		$cluster = new TWebSocketCluster('s1', $backplane);
		$server->setCluster($cluster);
		$server->setHandler(new TWebSocketHandler());

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$key = TWebSocketHandshake::generateKey();
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', $key));

		$server->serveOnce(0, 300000);   // accept + handshake registers the connection
		self::assertCount(1, $cluster->presence(), 'An accepted connection registers in the cluster.');
		self::assertGreaterThanOrEqual(1, $backplane->ticks, 'The serve loop pumps the backplane each iteration.');

		$clientWs = new TWebSocketConnection($client, true);
		$clientWs->close(1000);          // a clean WebSocket Close frame ends the session
		$server->serveOnce(0, 300000);   // reads the close, ends the session and unregisters
		self::assertCount(0, $cluster->presence(), 'A closed connection unregisters from the cluster.');

		$client->close();
		$server->close();
	}

	public function testClusterPathUpgradeIsAcceptedAsAMeshPeer()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$mesh = new TMeshBackplane();
		$mesh->setSecret('shh');
		$cluster = new TWebSocketCluster('srv', $mesh);
		$server->setCluster($cluster);
		$server->setHandler(new TWebSocketHandler());

		$opened = 0;
		$server->attachEventHandler('onConnection', function () use (&$opened): void {
			$opened++;
		});

		$peer = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$key = TWebSocketHandshake::generateKey();
		$proof = base64_encode(hash_hmac('sha256', 'handshake:' . $key, sha1('shh'), true));
		$peer->write(TWebSocketHandshake::buildClientRequest('cluster', '/cluster', $key, ['X-Cluster-Auth' => $proof]));
		$server->serveOnce(0, 300000);

		self::assertSame(1, $mesh->getPeerCount(), 'A /cluster upgrade joins the mesh as a peer.');
		self::assertCount(0, $cluster->presence(), 'A peer is not registered as a client.');
		self::assertSame(0, $opened, 'A peer does not raise onConnection.');

		$peer->close();
		$server->close();
	}

	public function testMeshPeerWithAValidSecretIsAccepted()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$mesh = new TMeshBackplane();
		$mesh->setSecret('shh');
		$server->setCluster(new TWebSocketCluster('srv', $mesh));
		$server->setHandler(new TWebSocketHandler());

		$peer = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$key = TWebSocketHandshake::generateKey();
		$proof = base64_encode(hash_hmac('sha256', 'handshake:' . $key, sha1('shh'), true));
		$peer->write(TWebSocketHandshake::buildClientRequest('cluster', '/cluster', $key, ['X-Cluster-Auth' => $proof]));
		$server->serveOnce(0, 300000);

		self::assertSame(1, $mesh->getPeerCount(), 'A peer proving the secret joins the mesh.');
		$peer->close();
		$server->close();
	}

	public function testMeshPeerWithoutAValidSecretIsRejected()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$mesh = new TMeshBackplane();
		$mesh->setSecret('shh');
		$server->setCluster(new TWebSocketCluster('srv', $mesh));
		$server->setHandler(new TWebSocketHandler());

		$peer = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$key = TWebSocketHandshake::generateKey();
		$peer->write(TWebSocketHandshake::buildClientRequest('cluster', '/cluster', $key, ['X-Cluster-Auth' => 'forged']));
		$server->serveOnce(0, 300000);

		self::assertSame(0, $mesh->getPeerCount(), 'A peer that cannot prove the secret is refused.');
		$peer->close();
		$server->close();
	}

	public function testClientPathStillRegistersWhenAMeshIsConfigured()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$mesh = new TMeshBackplane();
		$cluster = new TWebSocketCluster('srv', $mesh);
		$server->setCluster($cluster);
		$server->setHandler(new TWebSocketHandler());

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$key = TWebSocketHandshake::generateKey();
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', $key));
		$server->serveOnce(0, 300000);

		self::assertCount(1, $cluster->presence(), 'A non-cluster path is still a client.');
		self::assertSame(0, $mesh->getPeerCount(), 'A client is not a mesh peer.');

		$client->close();
		$server->close();
	}

	public function testAsyncDialEstablishesAPeerWithoutBlocking()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$serverMesh = new TMeshBackplane();
		$serverMesh->setSecret('shh');
		$server->setCluster(new TWebSocketCluster('srv', $serverMesh));
		$server->setHandler(new TWebSocketHandler());

		$dialer = new TMeshBackplane();
		$dialer->setSecret('shh');   // the mesh is enabled only with a shared secret
		new TWebSocketCluster('dialer', $dialer);   // binds the mesh to a coordinator
		$dialer->connectPeer('tcp://127.0.0.1:' . $server->getPort());
		self::assertSame(0, $dialer->getPeerCount(), 'The dial returns immediately without blocking.');
		self::assertSame(1, $dialer->getPendingCount(), 'The dial is in flight.');

		// Drive the async connect and request send (no server accept yet, so nothing blocks).
		for ($i = 0; $i < 40; $i++) {
			$dialer->tick();
			usleep(1000);
		}
		$server->serveOnce(0, 200000);   // accept + handshake (the request is already buffered)
		for ($i = 0; $i < 40 && $dialer->getPeerCount() === 0; $i++) {
			$dialer->tick();
			usleep(1000);
		}

		self::assertSame(1, $dialer->getPeerCount(), 'The async dial establishes the peer.');
		self::assertSame(1, $serverMesh->getPeerCount(), 'The server accepted the peer.');

		$dialer->close();
		$server->close();
	}

	public function testAbruptDisconnectEndsSessionWithoutCrashing()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$cluster = new TWebSocketCluster('s1', new TNullBackplane());
		$server->setCluster($cluster);
		$server->setHandler(new TWebSocketHandler());

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$key = TWebSocketHandshake::generateKey();
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', $key));
		$server->serveOnce(0, 300000);
		self::assertCount(1, $cluster->presence());

		// Close without reading the 101; the unread receive buffer turns the close into a reset, so
		// the server's next read fails rather than reporting a clean end of stream.
		$client->close();
		$server->serveOnce(0, 300000);   // must end the session, not throw
		self::assertCount(0, $cluster->presence(), 'An abrupt disconnect ends the session and unregisters.');

		$server->close();
	}
}
