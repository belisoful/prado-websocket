<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TConfigurationException;
use Prado\Exceptions\TInvalidDataValueException;
use Prado\Exceptions\TSocketException;
use Prado\IO\Process\TProcess;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\Prado;
use Prado\Test\Unit\IO\Socket\WebSocket\TTestMonotonicClock;
use Prado\Util\Log\TLogger;

/**
 * A mesh that counts its resolver children and its dial attempts and, when given a script, runs it
 * in place of the system lookup.
 */
class ScriptedResolverMesh extends TMeshBackplane
{
	public int $spawned = 0;

	/** @var ?string PHP code whose stdout stands in for the resolver's, or null for the real lookup. */
	public ?string $script = null;

	/** @var TProcess[] */
	public array $children = [];

	/** @var array<string, int> The connectPeer() calls, per URI. */
	public array $dials = [];

	/** @var ?string The binary the search for a PHP CLI finds, or null for the real search. */
	public ?string $found = null;

	public function connectPeer(string $uri): void
	{
		$this->dials[$uri] = ($this->dials[$uri] ?? 0) + 1;
		parent::connectPeer($uri);
	}

	protected function startResolver(string $host): TProcess
	{
		$this->spawned++;
		if ($this->script === null) {
			return $this->children[] = parent::startResolver($host);
		}
		$process = TProcess::open([PHP_BINARY, '-n', '-r', $this->script, '--', $host], [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']]);
		$process->getStdin()?->close();
		$process->getStderr()?->close();
		stream_set_blocking($process->getStdout()->getResource(), false);
		return $this->children[] = $process;
	}

	protected function findResolverBinary(): string
	{
		return $this->found ?? parent::findResolverBinary();
	}
}

/**
 * Mesh dials to a hostname: the lookup runs in a child process the loop polls, so the dial never
 * blocks on DNS, and its address is cached.
 */
class TMeshBackplaneResolverTest extends TestCase
{
	/** The secret every node of these tests shares. */
	private const SECRET = 'shh';

	/** The seconds a test waits for a child process before it fails. */
	private const CHILD_WAIT = 10.0;

	/** @var ScriptedResolverMesh[] The dialers to close. */
	private array $dialers = [];

	/** @var TWebSocketServer[] The servers to close. */
	private array $servers = [];

	/** @var TSocketStream[] The listeners and raw sockets to close. */
	private array $sockets = [];

	/** Closes what a test opened, whether or not it passed, so no child, listener or dial outlives it. */
	protected function tearDown(): void
	{
		foreach ($this->dialers as $dialer) {
			$dialer->close();   // kills and reaps the lookups still running
			foreach ($dialer->children as $child) {
				if ($child->getIsRunning()) {
					$child->terminate(9);   // one a test started itself, or the fix under test failed to kill
				}
			}
		}
		foreach ($this->servers as $server) {
			$server->close();
		}
		foreach ($this->sockets as $socket) {
			$socket->close();
		}
		$this->dialers = $this->servers = $this->sockets = [];
	}

	/** @return array{0: TWebSocketServer, 1: TMeshBackplane} A server on 127.0.0.1 serving a mesh. */
	private function meshServer(): array
	{
		$this->servers[] = $server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$mesh = new TMeshBackplane();
		$mesh->setSecret(self::SECRET);
		$server->setCluster(new TWebSocketCluster('srv', $mesh));
		$server->setHandler(new TWebSocketHandler());
		return [$server, $mesh];
	}

	private function dialer(string $node = 'dialer'): ScriptedResolverMesh
	{
		$this->dialers[] = $dialer = new ScriptedResolverMesh();
		$dialer->setSecret(self::SECRET);
		new TWebSocketCluster($node, $dialer);
		return $dialer;
	}

	/**
	 * Binds a loopback listener that accepts connections into its backlog and never answers, so a
	 * dial to it stays pending in its WebSocket handshake.  The listener is held until the test ends,
	 * so no other process can take its port.
	 * @return TSocketStream The listener.
	 */
	private function listener(): TSocketStream
	{
		return $this->sockets[] = TSocketStream::bind('tcp://127.0.0.1:0', STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);
	}

	/** @return int The port of a new silent listener. */
	private function silentPort(): int
	{
		return $this->listener()->getLocalAddress()->getPort();
	}

	/** Ticks the dialer (and the server, when given) until the condition holds or the bound passes. */
	private function pumpUntil(TMeshBackplane $dialer, callable $done, ?TWebSocketServer $server = null): void
	{
		for ($i = 0; $i < 2000 && !$done(); $i++) {
			$dialer->tick();
			$server?->serveOnce(0, 5000);
			if ($server === null) {
				usleep(5000);
			}
		}
	}

	/** Waits until a resolver child has written to its stdout, which shows that it runs its program. */
	private function awaitOutput(TProcess $child): void
	{
		$read = [$child->getStdout()->getResource()];
		$write = $except = null;
		self::assertSame(1, stream_select($read, $write, $except, (int) self::CHILD_WAIT), 'The resolver child writes its first byte.');
	}

	/** Waits for a child to exit. */
	private function awaitExit(TProcess $child): bool
	{
		$deadline = hrtime(true) + (int) (self::CHILD_WAIT * 1_000_000_000);
		while ($child->getIsRunning() && hrtime(true) < $deadline) {
			usleep(2000);
		}
		return !$child->getIsRunning();
	}

	/** @return bool Whether this host has an IPv6 loopback address to listen on. */
	private function hasIpv6Loopback(): bool
	{
		$listener = @stream_socket_server('tcp://[::1]:0');
		if ($listener === false) {
			return false;
		}
		fclose($listener);
		return true;
	}

	/** @return array<int, array<int, mixed>> The warnings the dialer class has logged. */
	private function warnings(): array
	{
		return array_values(Prado::getLogger()->getLogs(TLogger::WARNING, ScriptedResolverMesh::class));
	}

	/**
	 * Accepts a link on a mesh and verifies it as node `$id`, by answering the mesh's challenge.
	 * @param TMeshBackplane $mesh
	 * @param string $id
	 * @return TWebSocketConnection The verified peer's end of the link.
	 */
	private function verifiedPeer(TMeshBackplane $mesh, string $id): TWebSocketConnection
	{
		[$rawPeer, $rawMesh] = TSocketStream::pair();
		$rawPeer->setBlocking(false);
		$rawMesh->setBlocking(false);
		$this->sockets[] = $rawPeer;
		$mesh->addPeer(new TWebSocketConnection($rawMesh, false), $rawMesh);
		$peer = new TWebSocketConnection($rawPeer, true);
		$nonce = '';
		foreach ($peer->feed($rawPeer->read(65536)) as $message) {
			$nonce = TWebSocketEnvelope::decode($message)->getPayload();
		}
		self::assertNotSame('', $nonce, 'The accepted peer is challenged.');
		$answer = base64_encode(hash_hmac('sha256', 'challenge:' . $nonce . "\0" . $id, sha1(self::SECRET), true));
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_RESPONSE, $id, $answer))->encode());
		$mesh->tick();
		self::assertSame(1, $mesh->getPeerCount());
		return $peer;
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
	}

	public function testASeedDialedByHostnameWhoseNodeAdvertisesItsAddressIsLinkedOnce()
	{
		[$server, $serverMesh] = $this->meshServer();
		$address = 'tcp://127.0.0.1:' . $server->getPort();
		$serverMesh->setAdvertise($address);
		$dialer = $this->dialer();
		$dialer->setPeers(['tcp://localhost:' . $server->getPort()]);
		$dialer->open();
		$this->pumpUntil($dialer, fn () => $dialer->getPeerCount() > 0 && $serverMesh->getPeerCount() > 0, $server);
		for ($i = 0; $i < 100; $i++) {   // the server's announce of its address arrives, and a second dial would complete
			$dialer->tick();
			$server->serveOnce(0, 2000);
		}

		self::assertSame(1, $dialer->getPeerCount(), 'The address the node advertises is another name of the link, not a second peer.');
		self::assertSame(1, $serverMesh->getPeerCount());
		self::assertSame(0, $dialer->getPendingCount(), 'The advertised address is not dialed.');
		self::assertArrayNotHasKey($address, $dialer->dials);
	}

	public function testASlowLookupDoesNotBlockTheTick()
	{
		[$server] = $this->meshServer();
		$dialer = $this->dialer();
		$dialer->script = 'echo " "; usleep(300000); echo json_encode(["127.0.0.1"]);';
		$dialer->connectPeer('tcp://peer.example:' . $server->getPort());
		$this->awaitOutput($dialer->children[0]);
		$dialer->tick();
		$dialer->tick();
		self::assertSame(1, $dialer->getResolvingCount(), 'The ticks return while the lookup is still running.');

		$this->pumpUntil($dialer, fn () => $dialer->getPeerCount() > 0, $server);
		self::assertSame(1, $dialer->getPeerCount(), 'The dial completes once the lookup answers.');
	}

	public function testOneLookupServesEveryDialToItsHostAndIsCached()
	{
		$dialer = $this->dialer();
		$dialer->script = 'echo json_encode(["127.0.0.1"]);';
		$dialer->connectPeer('tcp://peer.example:' . $this->silentPort());
		$dialer->connectPeer('tcp://peer.example:' . $this->silentPort());
		self::assertSame(1, $dialer->spawned, 'Dials to one host share its lookup.');
		$this->pumpUntil($dialer, fn () => $dialer->getResolvingCount() === 0);
		self::assertSame(2, $dialer->getPendingCount(), 'Both dials start once the host resolves.');

		$dialer->connectPeer('tcp://peer.example:' . $this->silentPort());
		self::assertSame(1, $dialer->spawned, 'A later dial uses the cached address.');
		self::assertSame(3, $dialer->getPendingCount(), 'It connects at once.');
	}

	public function testAFailedDialLooksTheHostUpAgain()
	{
		$dialer = $this->dialer();
		$dialer->script = 'echo json_encode(["127.0.0.1"]);';
		$listener = $this->listener();
		$uri = 'tcp://peer.example:' . $listener->getLocalAddress()->getPort();
		$dialer->connectPeer($uri);
		$this->pumpUntil($dialer, fn () => $dialer->getPendingCount() === 1);
		self::assertSame(1, $dialer->getPendingCount(), 'The dial starts once the host resolves.');
		$this->pumpUntil($dialer, function () use ($dialer, $listener): bool {
			$accepted = @stream_socket_accept($listener->getResource(), 0);
			if ($accepted !== false) {
				fclose($accepted);   // the peer hangs up before it answers the upgrade
			}
			return $dialer->getPendingCount() === 0;
		});
		self::assertSame(0, $dialer->getPendingCount(), 'The dial fails.');

		$dialer->connectPeer($uri);
		self::assertSame(2, $dialer->spawned, 'A failed dial drops the cached address, so a peer that moved is found.');
	}

	public function testALookupWithNoAddressFailsItsDials()
	{
		$dialer = $this->dialer();
		$dialer->script = 'echo json_encode([]);';
		$uri = 'tcp://nowhere.example:9';
		$dialer->connectPeer($uri);
		$this->pumpUntil($dialer, fn () => $dialer->getResolvingCount() === 0);
		self::assertSame(0, $dialer->getPendingCount(), 'Nothing is dialed.');
		self::assertSame([], $this->warnings(), 'A host without an address is no fault of the resolver.');

		$dialer->connectPeer($uri);
		self::assertSame(2, $dialer->spawned, 'The dial can be tried again.');
	}

	public function testALookupPastTheTimeoutIsAbandoned()
	{
		$clock = new TTestMonotonicClock(100);
		$dialer = $this->dialer();
		$dialer->setMonotonicClock($clock);
		$dialer->script = 'echo "["; sleep(60);';   // a resolver that starts answering, then hangs
		$dialer->connectPeer('tcp://slow.example:9');
		$this->awaitOutput($dialer->children[0]);
		$dialer->tick();
		self::assertSame(1, $dialer->getResolvingCount());

		$clock->advance($dialer->getTimeout() + 1);
		$dialer->tick();
		self::assertSame(0, $dialer->getResolvingCount(), 'The lookup is abandoned at the dial timeout.');
		self::assertSame(0, $dialer->getPendingCount());
		self::assertTrue($this->awaitExit($dialer->children[0]), 'The hung resolver is killed, not left running.');
		$dialer->tick();   // closes the killed child
	}

	public function testATimedOutLookupIsKilledWhateverItsAgeAndOutput()
	{
		$clock = new TTestMonotonicClock(100);
		$dialer = $this->dialer();
		$dialer->setMonotonicClock($clock);
		$dialer->setTimeout(0.3);
		$dialer->script = 'sleep(60);';   // a resolver blocked in DNS: it writes nothing
		$dialer->connectPeer('tcp://slow.example:9');
		$clock->advance(0.5);   // past the timeout at once: the child is younger than a second
		$dialer->tick();

		self::assertSame(0, $dialer->getResolvingCount());
		self::assertTrue($this->awaitExit($dialer->children[0]), 'A child that has written nothing and has only just started is killed too.');
	}

	public function testCloseKillsItsLookupsAndDoesNotWaitForThem()
	{
		$dialer = $this->dialer();
		$dialer->script = 'sleep(60);';
		$dialer->connectPeer('tcp://one.example:9');
		$dialer->connectPeer('tcp://two.example:9');
		$started = hrtime(true);
		$dialer->close();
		$seconds = (hrtime(true) - $started) / 1_000_000_000;
		$running = count(array_filter($dialer->children, fn (TProcess $child) => $child->getIsRunning()));

		self::assertSame(0, $running, 'No lookup outlives close(), so destroying the backplane waits for none.');
		self::assertSame(0, $dialer->getResolvingCount());
		self::assertLessThan(TMeshBackplane::RESOLVER_REAP_TIME + 10.0, $seconds, 'close() does not wait for a lookup to end.');
	}

	public function testALookupPastTheQueueCapIsPutOff()
	{
		$dialer = $this->dialer();
		$dialer->script = 'sleep(60);';
		for ($i = 0; $i < 256; $i++) {   // the most hosts that wait for a lookup
			$dialer->connectPeer("tcp://h{$i}.example:9");
		}
		$dialer->connectPeer('tcp://one-more.example:9');
		self::assertSame(256, $dialer->getResolvingCount(), 'A further host is not queued.');
		$dialer->connectPeer('tcp://h255.example:10');
		self::assertSame(256, $dialer->getResolvingCount(), 'A dial to a queued host joins its lookup.');
		self::assertSame(TMeshBackplane::MAX_RESOLVERS, $dialer->spawned);
	}

	public function testCloseForgetsTheResolvedAddresses()
	{
		$dialer = $this->dialer();
		$dialer->script = 'echo json_encode(["127.0.0.1"]);';
		$uri = 'tcp://peer.example:' . $this->silentPort();
		$dialer->connectPeer($uri);
		$this->pumpUntil($dialer, fn () => $dialer->getPendingCount() === 1);
		self::assertSame(1, $dialer->spawned);

		$dialer->close();
		$dialer->connectPeer($uri);
		self::assertSame(2, $dialer->spawned, 'A backplane that is opened again looks its hosts up again.');
	}

	public function testAtMostMaxResolversLookupsRunAtOnce()
	{
		$clock = new TTestMonotonicClock(100);
		$dialer = $this->dialer();
		$dialer->setMonotonicClock($clock);
		$dialer->script = 'sleep(60);';
		$hosts = TMeshBackplane::MAX_RESOLVERS * 2 + 3;
		for ($i = 0; $i < $hosts; $i++) {
			$dialer->connectPeer("tcp://h{$i}.example:9");
		}
		self::assertSame(TMeshBackplane::MAX_RESOLVERS, $dialer->spawned, 'Only MAX_RESOLVERS children are started.');
		self::assertSame($hosts, $dialer->getResolvingCount(), 'The other lookups wait their turn.');
		$dialer->tick();
		self::assertSame(TMeshBackplane::MAX_RESOLVERS, $dialer->spawned, 'A tick starts none while the first are running.');
		self::assertCount(TMeshBackplane::MAX_RESOLVERS, $dialer->getSources(), 'Only a running lookup is a source.');

		$clock->advance($dialer->getTimeout() + 1);
		$dialer->tick();   // the first lookups time out, and the next take their turn
		self::assertSame(TMeshBackplane::MAX_RESOLVERS * 2, $dialer->spawned);
		self::assertSame($hosts - TMeshBackplane::MAX_RESOLVERS, $dialer->getResolvingCount());
		$clock->advance($dialer->getTimeout() + 1);
		$dialer->tick();
		self::assertSame($hosts, $dialer->spawned, 'Every lookup gets its turn.');
		self::assertSame(3, $dialer->getResolvingCount());
	}

	public function testAQueuedLookupWaitsWithItsDials()
	{
		$dialer = $this->dialer();
		$dialer->script = 'usleep(50000); echo json_encode(["127.0.0.1"]);';
		for ($i = 0; $i < TMeshBackplane::MAX_RESOLVERS; $i++) {
			$dialer->connectPeer("tcp://h{$i}.example:" . $this->silentPort());
		}
		$queued = 'tcp://queued.example:' . $this->silentPort();
		$dialer->connectPeer($queued);
		$dialer->connectPeer($queued);   // already waiting on the lookup of its host
		self::assertSame(TMeshBackplane::MAX_RESOLVERS, $dialer->spawned);

		$this->pumpUntil($dialer, fn () => $dialer->getResolvingCount() === 0);
		self::assertSame(TMeshBackplane::MAX_RESOLVERS + 1, $dialer->spawned, 'The queued host is looked up once a child finishes.');
		self::assertSame(TMeshBackplane::MAX_RESOLVERS + 1, $dialer->getPendingCount(), 'Its dial starts when its lookup answers.');
	}

	public function testAConnectThatFailsAtOnceAfterTheLookupIsDialedWhenItIsAnnouncedAgain()
	{
		$dialer = $this->dialer();
		$dialer->script = 'echo json_encode(["127.0.0.1"]);';
		$peer = $this->verifiedPeer($dialer, 'A');
		$uri = 'tcp://peer.example';   // no port: the connect to the resolved address fails as it starts
		$announce = fn () => $peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'C', '', null, null, ['uri' => $uri]))->encode());

		$announce();
		$this->pumpUntil($dialer, fn () => $dialer->spawned === 1 && $dialer->getResolvingCount() === 0);
		self::assertSame(2, $dialer->dials[$uri] ?? 0, 'The URI is dialed when announced, and again once its host resolves.');
		self::assertSame(0, $dialer->getPendingCount(), 'The connect fails at once.');

		$announce();
		$dialer->tick();
		self::assertSame(3, $dialer->dials[$uri], 'A later announce of the URI dials it again.');
	}

	/** @return array<string, array{0: string}> */
	public static function validHosts(): array
	{
		return [
			'an underscore' => ['ws_node1'],
			'a compose service name' => ['proj_ws_1'],
			'underscores under a domain' => ['my_app_ws_1.my_net'],
			'a leading underscore' => ['_ws._tcp.example'],
			'a trailing dot' => ['node-a.example.'],
			'an IDNA label' => ['xn--bcher-kva.example'],
		];
	}

	/**
	 * @dataProvider validHosts
	 * @param string $host
	 */
	public function testAHostnameTheSystemResolverAcceptsIsLookedUp(string $host)
	{
		$dialer = $this->dialer();
		$dialer->script = 'echo json_encode([]);';
		$dialer->connectPeer("tcp://{$host}:9");
		self::assertSame(1, $dialer->spawned, 'The host is handed to the resolver.');
	}

	/** @return array<string, array{0: string}> */
	public static function invalidHosts(): array
	{
		return [
			'a leading hyphen' => ['-node.example'],
			'a trailing hyphen' => ['node-.example'],
			'an empty label' => ['node..example'],
			'a label over 63 characters' => [str_repeat('a', 64) . '.example'],
			'a name over 253 characters' => [implode('.', array_fill(0, 5, str_repeat('a', 60)))],
			'a non-ASCII label' => ['bücher.example'],
			'a zone on something that is no address' => ['[12345::1%eth0]'],
			'a zone without a name' => ['[fe80::1%]'],
		];
	}

	/**
	 * @dataProvider invalidHosts
	 * @param string $host
	 */
	public function testAnInvalidHostnameIsRefusedWithoutALookup(string $host)
	{
		$dialer = $this->dialer();
		try {
			$dialer->connectPeer("tcp://{$host}:9");
			self::fail('An invalid hostname is refused.');
		} catch (TInvalidDataValueException $e) {
			self::assertSame('websocket_backplane_mesh_host_invalid', $e->getErrorCode());
		}
		self::assertSame(0, $dialer->spawned);
		self::assertSame(0, $dialer->getPendingCount());
	}

	public function testAnIpAddressIsDialedWithoutALookup()
	{
		$dialer = $this->dialer();
		$dialer->connectPeer('tcp://127.0.0.1:' . $this->silentPort());
		self::assertSame(1, $dialer->getPendingCount());
		try {
			$dialer->connectPeer('tcp://[::1]:' . $this->silentPort());
		} catch (TSocketException $e) {
			self::assertFalse($this->hasIpv6Loopback(), 'Only a host without an IPv6 loopback refuses the connect: ' . $e->getMessage());
		}
		self::assertSame(0, $dialer->spawned, 'Neither address is looked up.');
		if ($this->hasIpv6Loopback()) {
			self::assertSame(2, $dialer->getPendingCount(), 'The IPv6 address is dialed as the IPv4 one is.');
		}
	}

	public function testAnIpv6AddressWithAZoneIsDialedAsWrittenWithoutALookup()
	{
		$dialer = $this->dialer();
		try {
			$dialer->connectPeer('tcp://[fe80::1%lo0]:9');   // an interface this host may not have
			self::assertSame(1, $dialer->getPendingCount(), 'The address is dialed with its zone.');
		} catch (TSocketException $e) {
			self::assertSame(0, $dialer->getPendingCount(), 'The system refuses a zone it does not have: ' . $e->getMessage());
		}
		self::assertSame(0, $dialer->spawned, 'A zoned address needs no lookup.');
	}

	public function testWithoutACliBinaryAHostnameDialIsRefusedAndReportedOnce()
	{
		Prado::getLogger()->deleteLogs(null, ScriptedResolverMesh::class);
		$dialer = $this->dialer();
		$dialer->found = '';   // as under php-fpm with no php beside it
		self::assertSame('', $dialer->getResolverBinary());
		foreach (['tcp://one.example:9', 'tcp://two.example:9'] as $uri) {
			try {
				$dialer->connectPeer($uri);
				self::fail('A hostname cannot be looked up without a PHP CLI binary.');
			} catch (TConfigurationException $e) {
				self::assertSame('websocket_backplane_mesh_resolver_missing', $e->getErrorCode());
			}
		}
		self::assertSame(0, $dialer->spawned);
		self::assertSame(0, $dialer->getResolvingCount());
		$warnings = $this->warnings();
		self::assertCount(1, $warnings, 'The cause is logged as a warning, once.');
		self::assertStringContainsString('ResolverBinary', $warnings[0][0]);

		$dialer->connectPeer('tcp://127.0.0.1:' . $this->silentPort());
		self::assertSame(1, $dialer->getPendingCount(), 'A peer given by IP address is dialed as before.');

		$dialer->setPeers(['tcp://seed.example:9']);
		$dialer->open();   // a hostname seed is skipped, not fatal
		self::assertSame(1, $dialer->getPendingCount());
	}

	public function testResolverBinaryIsTheCliBinaryUnlessSet()
	{
		$dialer = $this->dialer();
		self::assertSame(PHP_BINARY, $dialer->getResolverBinary(), 'Under the CLI the running binary resolves.');
		self::assertSame($dialer, $dialer->setResolverBinary('/opt/php/bin/php'));
		self::assertSame('/opt/php/bin/php', $dialer->getResolverBinary());
		$dialer->found = '/usr/bin/php';
		$dialer->setResolverBinary('');
		self::assertSame('/usr/bin/php', $dialer->getResolverBinary(), 'An empty value searches for the binary again.');
	}

	public function testTheSetResolverBinaryRunsTheLookup()
	{
		[$server, $serverMesh] = $this->meshServer();
		$dialer = $this->dialer();
		$dialer->found = '';
		$dialer->setResolverBinary(PHP_BINARY);
		$dialer->connectPeer('tcp://localhost:' . $server->getPort());
		$this->pumpUntil($dialer, fn () => $dialer->getPeerCount() > 0 && $serverMesh->getPeerCount() > 0, $server);
		self::assertSame(1, $dialer->getPeerCount(), 'The binary that was set looks the host up.');
	}

	public function testAResolverThatWritesNoAddressListIsReportedOnce()
	{
		Prado::getLogger()->deleteLogs(null, ScriptedResolverMesh::class);
		$dialer = $this->dialer();
		$dialer->script = 'echo "Usage: php-fpm [-n] [-e]"; exit(64);';   // what a binary that is not the CLI does with the lookup program
		$dialer->connectPeer('tcp://one.example:9');
		$dialer->connectPeer('tcp://two.example:9');
		$this->pumpUntil($dialer, fn () => $dialer->getResolvingCount() === 0);

		self::assertSame(0, $dialer->getPendingCount());
		$warnings = $this->warnings();
		self::assertCount(1, $warnings, 'A resolver that did not run the lookup is a warning, once.');
		self::assertStringContainsString('wrote no address list', $warnings[0][0]);
		self::assertStringContainsString('ResolverBinary', $warnings[0][0]);
		$dialer->connectPeer('tcp://one.example:9');
		self::assertSame(3, $dialer->spawned, 'The dial can be tried again.');
	}

	public function testTheResolverProgramAnswersWithAJsonListOfAddresses()
	{
		$dialer = $this->dialer();
		$dialer->connectPeer('tcp://localhost:9');
		$child = $dialer->children[0];
		$output = '';
		$deadline = hrtime(true) + (int) (self::CHILD_WAIT * 1_000_000_000);
		$stdout = $child->getStdout()->getResource();
		while (!feof($stdout) && hrtime(true) < $deadline) {
			$output .= (string) fread($stdout, 4096);
			usleep(2000);
		}
		$addresses = json_decode($output, true);
		self::assertIsArray($addresses, 'The program writes a JSON list: ' . $output);
		self::assertNotEmpty($addresses);
		foreach ($addresses as $address) {
			self::assertNotFalse(filter_var($address, FILTER_VALIDATE_IP), 'Every entry is an IP address.');
		}
		self::assertContains('127.0.0.1', $addresses, 'A name with an IPv4 address answers with it first.');
	}
}
