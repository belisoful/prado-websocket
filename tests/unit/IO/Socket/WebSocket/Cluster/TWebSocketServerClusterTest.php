<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\IO\IResource;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\IWebSocketBufferedBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\IO\Stream\TBufferStream;
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

/** A backplane whose one source holds queued output until the server reports it writable. */
class QueuedOutputBackplane extends TNullBackplane implements IWebSocketBufferedBackplane
{
	public bool $pending = true;
	/** @var IResource[] The sources the server asked to flush. */
	public array $flushed = [];

	public function __construct(public TSocketStream $source)
	{
		parent::__construct();
	}

	public function getSources(): array
	{
		return [$this->source];
	}

	public function getWriteSources(): array
	{
		return $this->pending ? [$this->source] : [];
	}

	public function flushSource(IResource $source): void
	{
		$this->flushed[] = $source;
		$this->pending = false;
	}
}

class TWebSocketServerClusterTest extends TestCase
{
	/**
	 * Pumps a server until a condition holds or about two seconds pass, so a slow tick costs pumps
	 * and a state that never comes fails the assertion that follows.
	 * @param TWebSocketServer $server The server to pump.
	 * @param callable $done Returns whether the awaited state is reached.
	 * @param ?callable $between Runs after each pump, for a peer the test drives by hand.
	 */
	private function pumpUntil(TWebSocketServer $server, callable $done, ?callable $between = null): void
	{
		for ($i = 0; $i < 100 && !$done(); $i++) {
			$server->serveOnce(0, 20000);
			if ($between !== null) {
				$between();
			}
		}
	}

	/**
	 * Pumps a server and reads a raw client without blocking until the server closes the transport
	 * or the pumps run out.
	 * @param TWebSocketServer $server The server to pump between reads.
	 * @param TSocketStream $client The client transport; it is switched to non-blocking.
	 * @return string The bytes read before the end of stream.
	 */
	private function readUntilClosed(TWebSocketServer $server, TSocketStream $client): string
	{
		$client->setBlocking(false);
		$bytes = $client->read(65536);
		for ($i = 0; $i < 100 && !$client->eof(); $i++) {
			$server->serveOnce(0, 20000);
			$bytes .= $client->read(65536);
		}
		return $bytes;
	}

	public function testAClusterSourceWithQueuedOutputWakesTheLoopAndIsFlushed()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		[$local, $remote] = TSocketStream::pair();
		$local->setBlocking(false);
		$backplane = new QueuedOutputBackplane($local);
		$server->setCluster(new TWebSocketCluster('s1', $backplane));
		$server->setHandler(new TWebSocketHandler());

		$server->serveOnce(0);   // registers the cluster source and arms it for writability
		self::assertSame([], $backplane->flushed, 'Nothing is flushed before the loop sees the source writable.');

		$started = microtime(true);
		$server->serveOnce(1);   // the armed, writable source wakes the loop at once
		self::assertLessThan(0.5, microtime(true) - $started, 'A cluster source with queued output does not wait out the tick timeout.');
		self::assertSame([$local], $backplane->flushed, 'The writable source is flushed through the backplane.');

		$server->serveOnce(0, 20000);
		self::assertCount(1, $backplane->flushed, 'A drained source is disarmed and not flushed again.');

		$remote->close();
		$local->close();
		$server->close();
	}

	public function testALargeMeshMessageDrainsThroughTheServeLoop()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$serverMesh = new TMeshBackplane();
		$serverMesh->setSecret('shh');
		$serverCluster = new TWebSocketCluster('srv', $serverMesh);
		$server->setCluster($serverCluster);
		$server->setHandler(new TWebSocketHandler());

		$dialer = new TMeshBackplane();
		$dialer->setSecret('shh');
		$dialerCluster = new TWebSocketCluster('dialer', $dialer);
		$sink = new TBufferStream();
		$dialerCluster->register(new TWebSocketConnection($sink, false));
		$dialer->connectPeer('tcp://127.0.0.1:' . $server->getPort());
		// Accept, handshake, and the mutual challenge: the server mirrors the dialer's client once each end has verified the other.
		$this->pumpUntil($server, fn () => count($serverCluster->presence()) === 1, fn () => $dialer->tick());
		self::assertSame(1, $serverMesh->getPeerCount(), 'The dialer is linked.');
		self::assertCount(1, $serverCluster->presence(), 'The link is verified at both ends.');

		$serverCluster->broadcast(str_repeat('x', 8 * 1024 * 1024));   // larger than the socket buffers
		self::assertNotSame([], $serverMesh->getWriteSources(), 'The kernel does not take the message at once.');
		$deadline = hrtime(true) + 20_000_000_000;   // a time budget: an iteration count measures the machine, not the drain
		while ($sink->getSize() === 0 && hrtime(true) < $deadline) {
			$server->serveOnce(0, 2000);   // no further cluster traffic: only the loop, woken by writability, drains the link
			$dialer->tick();
		}
		self::assertGreaterThan(8 * 1024 * 1024, $sink->getSize(), 'The whole message reaches the remote client.');
		self::assertSame([], $serverMesh->getWriteSources(), 'Nothing stays queued on the link.');

		$dialer->close();
		$server->close();
	}

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
		$this->pumpUntil($server, fn () => count($cluster->presence()) === 1);   // the accept, the request and the reply can take separate ticks
		self::assertCount(1, $cluster->presence());
		self::assertStringContainsString('101', $client->read(4096));

		(new TWebSocketConnection($client, true))->close(1000);
		$this->pumpUntil($server, fn () => $server->getConnectionCount() === 0);   // the unregister throws inside the backplane; the loop must survive it

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

		$this->pumpUntil($server, fn () => count($cluster->presence()) === 1);   // accept + handshake registers the connection
		self::assertCount(1, $cluster->presence(), 'An accepted connection registers in the cluster.');
		self::assertGreaterThanOrEqual(1, $backplane->ticks, 'The serve loop pumps the backplane each iteration.');

		$clientWs = new TWebSocketConnection($client, true);
		$clientWs->close(1000);          // a clean WebSocket Close frame ends the session
		$this->pumpUntil($server, fn () => count($cluster->presence()) === 0);   // reads the close, ends the session and unregisters
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
		$this->pumpUntil($server, fn () => $mesh->getPeerCount() === 1);   // the accept, the request and the reply can take separate ticks

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
		$this->pumpUntil($server, fn () => $mesh->getPeerCount() === 1);   // the accept, the request and the reply can take separate ticks

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
		$response = $this->readUntilClosed($server, $peer);

		self::assertStringStartsWith('HTTP/1.1 403 ', $response, 'A peer that cannot prove the secret is answered with 403.');
		self::assertStringNotContainsString('Sec-WebSocket-Accept', $response, 'The upgrade is not completed.');
		self::assertTrue($peer->eof(), 'The server closes the refused transport.');
		self::assertSame(0, $mesh->getPeerCount(), 'The mesh holds no link for it.');
		self::assertSame(0, $server->getLoad(), 'The server holds no connection for it.');
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
		for ($i = 0; $i < 100 && count($cluster->presence()) === 0; $i++) {
			$server->serveOnce(0, 20000);   // the accept and the request can land in different ticks
		}

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

		// Drive the async connect and request send, interleaved with server pumps (accept + handshake),
		// until both ends hold the peer; a slow tick costs pumps, and a dial that never completes ends the loop.
		for ($i = 0; $i < 200 && ($dialer->getPeerCount() === 0 || $serverMesh->getPeerCount() === 0); $i++) {
			$dialer->tick();
			$server->serveOnce(0, 10000);
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
		$this->pumpUntil($server, fn () => count($cluster->presence()) === 1);   // the accept, the request and the reply can take separate ticks
		self::assertCount(1, $cluster->presence());

		// Close without reading the 101; the unread receive buffer turns the close into a reset, so
		// the server's next read fails rather than reporting a clean end of stream.
		$client->close();
		$this->pumpUntil($server, fn () => count($cluster->presence()) === 0);   // must end the session, not throw
		self::assertCount(0, $cluster->presence(), 'An abrupt disconnect ends the session and unregisters.');

		$server->close();
	}
}
