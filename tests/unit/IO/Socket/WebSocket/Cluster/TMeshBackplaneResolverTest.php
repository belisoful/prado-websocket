<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TInvalidDataValueException;
use Prado\IO\Process\TProcess;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\Test\Unit\IO\Socket\WebSocket\TTestMonotonicClock;

/** A mesh that counts its resolver children and, when given a script, runs it in place of the system lookup. */
class ScriptedResolverMesh extends TMeshBackplane
{
	public int $spawned = 0;

	/** @var ?string PHP code whose stdout stands in for the resolver's, or null for the real lookup. */
	public ?string $script = null;

	/** @var TProcess[] */
	public array $children = [];

	protected function startResolver(string $host): TProcess
	{
		$this->spawned++;
		if ($this->script === null) {
			return $this->children[] = parent::startResolver($host);
		}
		$process = TProcess::open([PHP_BINARY, '-n', '-r', $this->script], [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']]);
		stream_set_blocking($process->getStdout()->getResource(), false);
		return $this->children[] = $process;
	}
}

/**
 * Mesh dials to a hostname: the lookup runs in a child process the loop polls, so the dial never
 * blocks on DNS, and its address is cached.
 */
class TMeshBackplaneResolverTest extends TestCase
{
	/** @return array{0: TWebSocketServer, 1: TMeshBackplane} A server on 127.0.0.1 serving a mesh. */
	private function meshServer(): array
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$mesh = new TMeshBackplane();
		$mesh->setSecret('shh');
		$server->setCluster(new TWebSocketCluster('srv', $mesh));
		$server->setHandler(new TWebSocketHandler());
		return [$server, $mesh];
	}

	private function dialer(): ScriptedResolverMesh
	{
		$dialer = new ScriptedResolverMesh();
		$dialer->setSecret('shh');
		new TWebSocketCluster('dialer', $dialer);
		return $dialer;
	}

	/** @return int A loopback port nothing listens on. */
	private function closedPort(): int
	{
		$listener = TSocketStream::bind('tcp://127.0.0.1:0', STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);
		$port = $listener->getLocalAddress()->getPort();
		$listener->close();
		return $port;
	}

	/**
	 * Binds a listener that accepts connections into its backlog but never answers, so a dial to it
	 * stays pending in its WebSocket handshake.
	 * @param TSocketStream[] $keep The listeners to hold open.
	 */
	private function silentPort(array &$keep): int
	{
		$keep[] = $listener = TSocketStream::bind('tcp://127.0.0.1:0', STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);
		return $listener->getLocalAddress()->getPort();
	}

	/** Ticks the dialer (and the server, when given) until the condition holds or the bound passes. */
	private function pumpUntil(TMeshBackplane $dialer, callable $done, ?TWebSocketServer $server = null): void
	{
		for ($i = 0; $i < 600 && !$done(); $i++) {
			$dialer->tick();
			$server?->serveOnce(0, 5000);
			if ($server === null) {
				usleep(5000);
			}
		}
	}

	public function testAHostnameIsResolvedOffTheLoopAndDialed()
	{
		[$server, $serverMesh] = $this->meshServer();
		$dialer = $this->dialer();
		$dialer->connectPeer('tcp://localhost:' . $server->getPort());
		self::assertSame(1, $dialer->getResolvingCount(), 'The dial waits on a lookup.');
		self::assertSame(0, $dialer->getPendingCount(), 'No socket is opened before the host resolves.');
		self::assertContains($dialer->children[0]->getStdout(), $dialer->getSources(), 'The lookup\'s output wakes the loop.');

		$this->pumpUntil($dialer, fn () => $dialer->getPeerCount() > 0 && $serverMesh->getPeerCount() > 0, $server);
		self::assertSame(1, $dialer->getPeerCount(), 'localhost resolves through the system resolver and the dial links.');
		self::assertSame(1, $serverMesh->getPeerCount());
		self::assertSame(0, $dialer->getResolvingCount());
		$dialer->close();
		$server->close();
	}

	public function testASlowLookupDoesNotBlockTheTick()
	{
		[$server, $serverMesh] = $this->meshServer();
		$dialer = $this->dialer();
		$dialer->script = 'usleep(300000); echo json_encode(["127.0.0.1"]);';
		$dialer->connectPeer('tcp://peer.example:' . $server->getPort());
		$dialer->tick();
		$dialer->tick();
		self::assertSame(1, $dialer->getResolvingCount(), 'The ticks return while the lookup is still running.');
		self::assertTrue($dialer->children[0]->getIsRunning());

		$this->pumpUntil($dialer, fn () => $dialer->getPeerCount() > 0, $server);
		self::assertSame(1, $dialer->getPeerCount(), 'The dial completes once the lookup answers.');
		$dialer->close();
		$server->close();
	}

	public function testOneLookupServesEveryDialToItsHostAndIsCached()
	{
		$dialer = $this->dialer();
		$dialer->script = 'echo json_encode(["127.0.0.1"]);';
		$listeners = [];
		$dialer->connectPeer('tcp://peer.example:' . $this->silentPort($listeners));
		$dialer->connectPeer('tcp://peer.example:' . $this->silentPort($listeners));
		self::assertSame(1, $dialer->spawned, 'Dials to one host share its lookup.');
		$this->pumpUntil($dialer, fn () => $dialer->getResolvingCount() === 0);
		self::assertSame(2, $dialer->getPendingCount(), 'Both dials start once the host resolves.');

		$dialer->connectPeer('tcp://peer.example:' . $this->silentPort($listeners));
		self::assertSame(1, $dialer->spawned, 'A later dial uses the cached address.');
		self::assertSame(3, $dialer->getPendingCount(), 'It connects at once.');
		$dialer->close();
		foreach ($listeners as $listener) {
			$listener->close();
		}
	}

	public function testAFailedDialLooksTheHostUpAgain()
	{
		$dialer = $this->dialer();
		$dialer->script = 'echo json_encode(["127.0.0.1"]);';
		$uri = 'tcp://peer.example:' . $this->closedPort();
		$dialer->connectPeer($uri);
		$this->pumpUntil($dialer, fn () => $dialer->getResolvingCount() === 0 && $dialer->getPendingCount() === 0);
		self::assertSame(0, $dialer->getPendingCount(), 'The dial to the closed port fails.');

		$dialer->connectPeer($uri);
		self::assertSame(2, $dialer->spawned, 'A failed dial drops the cached address, so a peer that moved is found.');
		$dialer->close();
	}

	public function testALookupWithNoAddressFailsItsDials()
	{
		$dialer = $this->dialer();
		$dialer->script = 'echo json_encode([]);';
		$uri = 'tcp://nowhere.example:9';
		$dialer->connectPeer($uri);
		$this->pumpUntil($dialer, fn () => $dialer->getResolvingCount() === 0);
		self::assertSame(0, $dialer->getPendingCount(), 'Nothing is dialed.');

		$dialer->connectPeer($uri);
		self::assertSame(2, $dialer->spawned, 'The dial can be tried again.');
		$dialer->close();
	}

	public function testALookupPastTheTimeoutIsAbandoned()
	{
		$clock = new TTestMonotonicClock(100);
		$dialer = $this->dialer();
		$dialer->setMonotonicClock($clock);
		$dialer->script = 'echo "["; sleep(5);';   // a resolver that starts answering, then hangs
		$dialer->connectPeer('tcp://slow.example:9');
		usleep(500000);   // let the child start and write its first byte
		$dialer->tick();
		self::assertSame(1, $dialer->getResolvingCount());

		$clock->advance($dialer->getTimeout() + 1);
		$dialer->tick();
		self::assertSame(0, $dialer->getResolvingCount(), 'The lookup is abandoned at the dial timeout.');
		self::assertSame(0, $dialer->getPendingCount());
		$child = $dialer->children[0];
		for ($i = 0; $i < 200 && $child->getIsRunning(); $i++) {
			usleep(10000);   // the child, sent SIGTERM, exits
		}
		self::assertFalse($child->getIsRunning(), 'The hung resolver is terminated rather than left running.');
		$dialer->close();
	}

	public function testAnInvalidHostnameIsRefusedWithoutALookup()
	{
		$dialer = $this->dialer();
		try {
			$dialer->connectPeer('tcp://bad_host:9');
			self::fail('An invalid hostname is refused.');
		} catch (TInvalidDataValueException $e) {
			self::assertSame('websocket_backplane_mesh_host_invalid', $e->getErrorCode());
		}
		self::assertSame(0, $dialer->spawned);
		$dialer->close();
	}

	public function testAnIpAddressIsDialedWithoutALookup()
	{
		$dialer = $this->dialer();
		$dialer->connectPeer('tcp://127.0.0.1:' . $this->closedPort());
		$dialer->connectPeer('tcp://[::1]:' . $this->closedPort());
		self::assertSame(0, $dialer->spawned);
		self::assertSame(2, $dialer->getPendingCount());
		$dialer->close();
	}
}
