<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TConfigurationException;
use Prado\IO\Socket\WebSocket\Cluster\TRedisBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Stream\TBufferStream;
use Prado\Test\Unit\IO\Socket\WebSocket\TTestMonotonicClock;
use Prado\Util\Clock\TMockClock;

/**
 * An in-memory stand-in for a Redis server, covering the commands the backplane issues.  Every
 * backplane talks to it through its own {@see FakeRedisConnection}, which holds that client's
 * transaction state, as a real connection does.
 */
class FakeRedis
{
	/** @var array<string, array<string, true>> */
	public array $sets = [];
	/** @var array<string, array<string, string>> */
	public array $hashes = [];
	/** @var array<string, string[]> */
	public array $lists = [];
	/** @var array<string, string> */
	public array $strings = [];
	/** @var array<string, int> The expiry, in seconds, last set on each key. */
	public array $ttls = [];
	/** @var array<string, int> A write count per key, which a WATCH compares. */
	public array $versions = [];
	/** @var bool Whether every command fails, as when the server is down. */
	public bool $failing = false;
	public int $connects = 0;
	public int $closes = 0;
	/** @var bool The results of connect(), auth() and select(). */
	public bool $connectOk = true;
	public bool $authOk = true;
	public bool $selectOk = true;
	/** @var array<int, int|string> The passwords and databases the client was given. */
	public array $calls = [];
	/** @var array<int, array{0: string, 1: array<int, mixed>, 2: callable}> Hooks run once after a matching command. */
	private array $hooks = [];
	/** @var callable[] Hooks a transaction deferred until it completed. */
	private array $deferred = [];
	private int $inExec = 0;

	/** Forgets everything, as a restarted Redis would. */
	public function flush(): void
	{
		$this->sets = $this->hashes = $this->lists = $this->strings = $this->ttls = [];
	}

	/**
	 * Runs a hook once, after the next command named $command whose leading arguments are $args.  A
	 * command inside a MULTI transaction runs its hook after the whole transaction, which is atomic.
	 * @param string $command
	 * @param array<int, mixed> $args
	 * @param callable $hook
	 */
	public function after(string $command, array $args, callable $hook): void
	{
		$this->hooks[] = [$command, $args, $hook];
	}

	public function guard(): void
	{
		if ($this->failing) {
			throw new \RuntimeException('redis is down');
		}
	}

	/**
	 * Runs one data command.
	 * @param string $name
	 * @param array<int, mixed> $args
	 */
	public function run(string $name, array $args): mixed
	{
		$this->guard();
		$result = $this->{'do' . ucfirst($name)}(...$args);
		foreach ($this->hooks as $i => [$command, $prefix, $hook]) {
			if ($command === $name && array_slice($args, 0, count($prefix)) === $prefix) {
				unset($this->hooks[$i]);
				if ($this->inExec > 0) {
					$this->deferred[] = $hook;
				} else {
					$hook();
				}
			}
		}
		return $result;
	}

	/**
	 * Runs a queued transaction or pipeline with no other client's command in between.
	 * @param array<int, array{0: string, 1: array<int, mixed>}> $queue
	 * @return array<int, mixed>
	 */
	public function runAtomically(array $queue): array
	{
		$results = [];
		$this->inExec++;
		try {
			foreach ($queue as [$name, $args]) {
				$results[] = $this->run($name, $args);
			}
		} finally {
			$this->inExec--;
		}
		$deferred = $this->deferred;
		$this->deferred = [];
		foreach ($deferred as $hook) {
			$hook();
		}
		return $results;
	}

	public function connect(string $host, int $port, float $timeout): bool
	{
		$this->guard();
		$this->connects++;
		return $this->connectOk;
	}

	public function auth(string $password): bool
	{
		$this->calls[] = $password;
		return $this->authOk;
	}

	public function select(int $database): bool
	{
		$this->calls[] = $database;
		return $this->selectOk;
	}

	public function close(): bool
	{
		$this->closes++;
		return true;
	}

	private function touch(string $key): void
	{
		$this->versions[$key] = ($this->versions[$key] ?? 0) + 1;
	}

	private function doSAdd(string $key, string $member): int
	{
		if (isset($this->sets[$key][$member])) {
			return 0;
		}
		$this->sets[$key][$member] = true;
		$this->touch($key);
		return 1;
	}

	private function doSRem(string $key, string $member): int
	{
		if (!isset($this->sets[$key][$member])) {
			return 0;
		}
		unset($this->sets[$key][$member]);
		$this->touch($key);
		return 1;
	}

	private function doSMembers(string $key): array
	{
		return array_map('strval', array_keys($this->sets[$key] ?? []));
	}

	private function doSetex(string $key, int $ttl, string $value): bool
	{
		$this->strings[$key] = $value;
		$this->ttls[$key] = $ttl;
		$this->touch($key);
		return true;
	}

	private function doExpire(string $key, int $ttl): bool
	{
		$this->ttls[$key] = $ttl;
		return true;
	}

	private function doExists(string $key): int
	{
		return isset($this->strings[$key]) ? 1 : 0;
	}

	private function doDel(string $key): int
	{
		$had = isset($this->sets[$key]) || isset($this->hashes[$key]) || isset($this->lists[$key]) || isset($this->strings[$key]);
		unset($this->sets[$key], $this->hashes[$key], $this->lists[$key], $this->strings[$key], $this->ttls[$key]);
		if ($had) {
			$this->touch($key);
		}
		return $had ? 1 : 0;
	}

	private function doHSet(string $key, string $field, string $value): int
	{
		$new = !isset($this->hashes[$key][$field]);
		$this->hashes[$key][$field] = $value;
		$this->touch($key);
		return $new ? 1 : 0;
	}

	private function doHDel(string $key, string $field): int
	{
		if (!isset($this->hashes[$key][$field])) {
			return 0;
		}
		unset($this->hashes[$key][$field]);
		$this->touch($key);
		return 1;
	}

	private function doHGet(string $key, string $field): string|false
	{
		return $this->hashes[$key][$field] ?? false;
	}

	private function doHGetAll(string $key): array
	{
		return $this->hashes[$key] ?? [];
	}

	private function doRPush(string $key, string $value): int
	{
		$this->lists[$key][] = $value;
		$this->touch($key);
		return count($this->lists[$key]);
	}

	private function doLRange(string $key, int $start, int $stop): array
	{
		$list = $this->lists[$key] ?? [];
		return array_slice($list, $start, $stop < 0 ? null : $stop - $start + 1);
	}

	private function doLTrim(string $key, int $start, int $stop): bool
	{
		$list = $this->lists[$key] ?? [];
		$length = count($list);
		$start = $start < 0 ? max(0, $length + $start) : $start;
		$stop = $stop < 0 ? $length + $stop : min($stop, $length - 1);
		$this->lists[$key] = $start > $stop ? [] : array_slice($list, $start, $stop - $start + 1);
		$this->touch($key);
		return true;
	}
}

/**
 * One client connection to the {@see FakeRedis} server: it forwards each command, and holds its own
 * MULTI queue and WATCHed keys.  In MULTI or pipeline mode a command is queued and returns the
 * connection; EXEC runs the queue atomically, or returns false when a WATCHed key changed.
 */
class FakeRedisConnection
{
	/** @var ?array<int, array{0: string, 1: array<int, mixed>}> The queued commands, or null outside MULTI. */
	private ?array $queue = null;
	/** @var array<string, int> The WATCHed keys and their versions when watched. */
	private array $watched = [];
	public int $aborted = 0;

	public function __construct(public FakeRedis $server)
	{
	}

	public function connect(string $host, int $port, float $timeout): bool
	{
		return $this->server->connect($host, $port, $timeout);
	}

	public function setOption(int $option, mixed $value): bool
	{
		return true;
	}

	public function auth(string $password): bool
	{
		return $this->server->auth($password);
	}

	public function select(int $database): bool
	{
		return $this->server->select($database);
	}

	public function close(): bool
	{
		return $this->server->close();
	}

	public function multi(int $mode = 1): static
	{
		$this->server->guard();
		$this->queue = [];
		return $this;
	}

	public function exec(): array|false
	{
		$queue = $this->queue ?? [];
		$watched = $this->watched;
		$this->queue = null;
		$this->watched = [];
		foreach ($watched as $key => $version) {
			if (($this->server->versions[$key] ?? 0) !== $version) {
				$this->aborted++;
				return false;
			}
		}
		return $this->server->runAtomically($queue);
	}

	public function watch(array|string $key, string ...$others): bool
	{
		$this->server->guard();
		foreach ([...(array) $key, ...$others] as $name) {
			$this->watched[$name] = $this->server->versions[$name] ?? 0;
		}
		return true;
	}

	public function unwatch(): bool
	{
		$this->watched = [];
		return true;
	}

	/**
	 * Queues a command inside MULTI, or runs it.
	 * @param string $name
	 * @param array<int, mixed> $args
	 */
	public function __call(string $name, array $args): mixed
	{
		if ($this->queue !== null) {
			$this->queue[] = [$name, $args];
			return $this;
		}
		return $this->server->run($name, $args);
	}
}

/** A backplane that connects to the fake instead of a real server. */
class FakeRedisBackplane extends TRedisBackplane
{
	public function __construct(public FakeRedis $fake)
	{
		parent::__construct();
	}

	protected function createRedis(): object
	{
		return new FakeRedisConnection($this->fake);
	}
}

/** A backplane on a host without ext-redis. */
class NoExtensionRedisBackplane extends TRedisBackplane
{
	protected function hasRedisExtension(): bool
	{
		return false;
	}
}

class TRedisBackplaneTest extends TestCase
{
	private FakeRedis $redis;
	private TMockClock $clock;
	private TTestMonotonicClock $mono;
	private string $prefix;

	protected function setUp(): void
	{
		$this->redis = new FakeRedis();
		$this->clock = new TMockClock();
		$this->clock->setMicrotime(1000.0);
		$this->mono = new TTestMonotonicClock(1000.0);
		$this->prefix = (new TRedisBackplane())->getPrefix();
	}

	/** @return array{0: TWebSocketCluster, 1: FakeRedisBackplane} A cluster node on the fake Redis and its backplane. */
	private function makeNode(string $nodeId): array
	{
		$backplane = new FakeRedisBackplane($this->redis);
		$backplane->setClock($this->clock);
		$backplane->setMonotonicClock($this->mono);
		return [new TWebSocketCluster($nodeId, $backplane), $backplane];
	}

	/** @return array{0: TWebSocketConnection, 1: TBufferStream} A server-side connection and its sink. */
	private function makeConnection(): array
	{
		$stream = new TBufferStream();
		return [new TWebSocketConnection($stream, false), $stream];
	}

	/** @return TWebSocketEnvelope[] The envelopes queued in a node's inbox. */
	private function inbox(string $node): array
	{
		return array_values(array_filter(array_map(fn ($line) => TWebSocketEnvelope::decode($line), $this->redis->lists[$this->prefix . 'inbox:' . $node] ?? [])));
	}

	public function testOpenJoinsTheRegistryAndSeedsThePresenceMirror()
	{
		$this->redis->hashes[$this->prefix . 'presence'] = ['n2-1' => '{"node":"n2"}'];
		$this->redis->sets[$this->prefix . 'nodes'] = ['n2' => true];
		[$cluster] = $this->makeNode('n1');
		$cluster->open();

		self::assertSame(1, $this->redis->connects);
		self::assertArrayHasKey('n1', $this->redis->sets[$this->prefix . 'nodes'], 'The node joins the registry.');
		self::assertArrayHasKey($this->prefix . 'node:n1', $this->redis->strings, 'The heartbeat key is written.');
		self::assertSame(['n2-1'], array_keys($cluster->presence()), 'Remote presence is seeded into the mirror.');
	}

	public function testOpenPurgesAPreviousIncarnationOfTheSameNodeId()
	{
		// The node id restarted inside its TTL: its old clients and channel interest are still in Redis.
		$this->redis->hashes[$this->prefix . 'presence'] = ['n1-old-1' => '{"node":"n1"}', 'n2-1' => '{"node":"n2"}'];
		$this->redis->sets[$this->prefix . 'ch:news'] = ['n1' => true, 'n2' => true];
		$this->redis->sets[$this->prefix . 'nodech:n1'] = ['news' => true];
		$this->redis->sets[$this->prefix . 'nodes'] = ['n1' => true, 'n2' => true];
		$this->redis->lists[$this->prefix . 'inbox:n1'] = [(new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'n2', 'late', 'news'))->encode()];
		[$cluster] = $this->makeNode('n1');
		$cluster->open();

		self::assertSame(['n2-1'], array_keys($this->redis->hashes[$this->prefix . 'presence']), "The previous incarnation's clients are purged; the other node's remain.");
		self::assertSame(['n2'], array_keys($this->redis->sets[$this->prefix . 'ch:news']), 'Stale channel interest is withdrawn.');
		self::assertSame(['n2-1'], array_keys($cluster->presence()), 'The mirror never sees the phantom clients.');
		self::assertArrayHasKey('n1', $this->redis->sets[$this->prefix . 'nodes'], 'The node re-joins the registry.');
		self::assertCount(1, $this->redis->lists[$this->prefix . 'inbox:n1'], 'The inbox is kept, to be drained.');
	}

	public function testReconnectRebuildsChannelInterestAndPresenceFromLocalState()
	{
		[$cluster] = $this->makeNode('n1');
		$cluster->open();
		[$connection] = $this->makeConnection();
		$clientId = $cluster->register($connection, ['user' => 'alice']);
		$cluster->subscribe($clientId, 'news');
		self::assertArrayHasKey($clientId, $this->redis->hashes[$this->prefix . 'presence']);
		self::assertArrayHasKey('n1', $this->redis->sets[$this->prefix . 'ch:news']);

		// Redis goes away: the next command fails and the backplane degrades to disconnected.
		$this->redis->failing = true;
		$cluster->tick();
		[$offline] = $this->makeConnection();
		$offlineId = $cluster->register($offline);   // registered during the outage; the write was a no-op
		$cluster->subscribe($offlineId, 'alerts');

		// Redis comes back empty (restarted); a surviving peer is already registered again.
		$this->redis->failing = false;
		$this->redis->flush();
		$this->redis->sets[$this->prefix . 'nodes'] = ['n2' => true];
		$this->redis->strings[$this->prefix . 'node:n2'] = '1';   // its heartbeat is live, so the prune keeps it
		$this->mono->setSeconds(1000.0 + TRedisBackplane::RECONNECT_INTERVAL + 1);
		$cluster->tick();

		self::assertSame(2, $this->redis->connects, 'The backplane reconnected.');
		self::assertArrayHasKey('n1', $this->redis->sets[$this->prefix . 'nodes']);
		self::assertArrayHasKey('n1', $this->redis->sets[$this->prefix . 'ch:news'] ?? [], 'Channel interest declared before the drop is re-declared.');
		self::assertArrayHasKey('n1', $this->redis->sets[$this->prefix . 'ch:alerts'] ?? [], 'Channel interest declared during the outage is declared on reconnect.');
		$presence = $this->redis->hashes[$this->prefix . 'presence'] ?? [];
		self::assertArrayHasKey($clientId, $presence, 'Presence from before the drop is re-put.');
		self::assertArrayHasKey($offlineId, $presence, 'Presence registered during the outage is put on reconnect.');
		self::assertSame('alice', json_decode($presence[$clientId], true)['user']);
		$announced = array_map(fn ($e) => $e->getClientId(), array_filter($this->inbox('n2'), fn ($e) => $e->getType() === TWebSocketEnvelope::PRESENCE_SET));
		self::assertEqualsCanonicalizing([$clientId, $offlineId], $announced, 'The surviving peer is told about every local client.');
	}

	public function testUnregisterDuringAnOutageDoesNotLeaveAPhantomAfterReconnect()
	{
		[$cluster] = $this->makeNode('n1');
		$cluster->open();
		[$connection] = $this->makeConnection();
		$clientId = $cluster->register($connection);

		$this->redis->failing = true;
		$cluster->tick();
		$cluster->unregister($clientId);   // the hash still holds the client; the delete was a no-op

		$this->redis->failing = false;
		$this->mono->setSeconds(1000.0 + TRedisBackplane::RECONNECT_INTERVAL + 1);
		$cluster->tick();

		self::assertArrayNotHasKey($clientId, $this->redis->hashes[$this->prefix . 'presence'] ?? [], 'A client that left during the outage is not resurrected.');
	}

	public function testPruneReapsADeadNodeAndDropsItsClientsFromTheMirror()
	{
		$this->redis->hashes[$this->prefix . 'presence'] = ['n2-1' => '{"node":"n2"}', 'n3-1' => '{"node":"n3"}'];
		$this->redis->sets[$this->prefix . 'nodes'] = ['n2' => true, 'n3' => true];
		$this->redis->sets[$this->prefix . 'ch:news'] = ['n2' => true];
		$this->redis->sets[$this->prefix . 'nodech:n2'] = ['news' => true];
		$this->redis->strings[$this->prefix . 'node:n3'] = '1';   // n3 is alive; n2's heartbeat expired
		[$cluster] = $this->makeNode('n1');
		$cluster->open();
		self::assertCount(2, $cluster->presence());

		$cluster->tick();   // the first tick runs the prune

		self::assertSame(['n3-1'], array_keys($cluster->presence()), "The dead node's clients leave the running node's mirror.");
		self::assertArrayNotHasKey('n2', $this->redis->sets[$this->prefix . 'nodes']);
		self::assertArrayNotHasKey('n2', $this->redis->sets[$this->prefix . 'ch:news'] ?? [], "The dead node's channel interest is withdrawn.");
		self::assertSame(['n3-1'], array_keys($this->redis->hashes[$this->prefix . 'presence']));
	}

	public function testInboxIsDrainedIntoTheCoordinator()
	{
		[$cluster] = $this->makeNode('n1');
		$cluster->open();
		[$connection, $sink] = $this->makeConnection();
		$clientId = $cluster->register($connection);
		$cluster->subscribe($clientId, 'news');
		$this->redis->lists[$this->prefix . 'inbox:n1'][] = (new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'n2', 'hello', 'news'))->encode();

		$cluster->tick();

		self::assertGreaterThan(0, $sink->getSize(), 'The queued publish reaches the local subscriber.');
		self::assertSame([], $this->redis->lists[$this->prefix . 'inbox:n1'], 'The drained entries are trimmed.');
	}

	public function testInboxIsCappedAtTheInboxLimit()
	{
		$this->redis->sets[$this->prefix . 'nodes'] = ['n2' => true];
		$this->redis->strings[$this->prefix . 'node:n2'] = '1';
		[$cluster, $backplane] = $this->makeNode('n1');
		$backplane->setInboxLimit(2);
		$cluster->open();

		$cluster->broadcast('one');
		$cluster->broadcast('two');
		$cluster->broadcast('three');

		$payloads = array_map(fn ($e) => $e->getPayload(), $this->inbox('n2'));
		self::assertSame(['two', 'three'], $payloads, 'A stalled inbox keeps only the newest entries.');
	}

	/** @return array{0: TWebSocketCluster, 1: FakeRedisBackplane} An opened cluster node on the fake Redis. */
	private function openNode(string $nodeId): array
	{
		[$cluster, $backplane] = $this->makeNode($nodeId);
		$cluster->open();
		return [$cluster, $backplane];
	}

	public function testANodeReapedWhileStalledRejoinsOnItsNextHeartbeat()
	{
		[$n1, $bp1] = $this->openNode('n1');
		[$n2] = $this->openNode('n2');
		[$connection, $sink] = $this->makeConnection();
		$clientId = $n1->register($connection);
		$n1->subscribe($clientId, 'news');
		$n2->tick();
		self::assertArrayHasKey($clientId, $n2->presence());

		// n1 stalls past the TTL: its heartbeat key expires and n2 reaps it.
		unset($this->redis->strings[$this->prefix . 'node:n1']);
		$this->mono->setSeconds(1000.0 + $bp1->getNodeTtl() + 1);
		$n2->tick();
		self::assertArrayNotHasKey('n1', $this->redis->sets[$this->prefix . 'nodes']);
		self::assertArrayNotHasKey('n1', $this->redis->sets[$this->prefix . 'ch:news'] ?? [], "The reap withdrew the stalled node's channel interest.");
		self::assertArrayNotHasKey($clientId, $n2->presence());

		// n1 resumes: its next heartbeat finds it missing from the registry and rebuilds.
		$n1->tick();
		self::assertArrayHasKey('n1', $this->redis->sets[$this->prefix . 'nodes'], 'The resumed node is back in the registry.');
		self::assertArrayHasKey('n1', $this->redis->sets[$this->prefix . 'ch:news'] ?? [], 'The resumed node re-declares its channel interest.');
		self::assertArrayHasKey($clientId, $this->redis->hashes[$this->prefix . 'presence'] ?? [], 'The resumed node re-puts its clients.');

		$n2->tick();
		self::assertArrayHasKey($clientId, $n2->presence(), "The peer learns the resumed node's client again.");
		$n2->publish('news', 'after');
		self::assertTrue($n2->sendToClient($clientId, 'direct'), "The peer can address the resumed node's client.");
		$before = $sink->getSize();
		$n1->tick();
		self::assertStringContainsString('after', substr((string) $sink, $before), 'The resumed node receives channel traffic.');
		self::assertStringContainsString('direct', substr((string) $sink, $before), 'The resumed node receives direct traffic.');
	}

	public function testEveryNodeDropsAReapedNodesClients()
	{
		[$a] = $this->openNode('A');
		[$b] = $this->openNode('B');
		[$c, $bpC] = $this->openNode('C');
		[$connection] = $this->makeConnection();
		$clientId = $c->register($connection);
		$a->tick();
		$b->tick();
		self::assertArrayHasKey($clientId, $a->presence());
		self::assertArrayHasKey($clientId, $b->presence());

		// C crashes and its heartbeat key expires.  A prunes first; C is then gone from the registry, so B's prune never finds it.
		unset($this->redis->strings[$this->prefix . 'node:C']);
		$this->mono->setSeconds(1000.0 + $bpC->getNodeTtl() + 1);
		$a->tick();
		$b->tick();

		self::assertArrayNotHasKey($clientId, $a->presence(), "The reaping node drops the dead node's client.");
		self::assertArrayNotHasKey($clientId, $b->presence(), "Every other node drops the dead node's client too.");
	}

	public function testAClientThatLeftDuringAnOutageIsDroppedOnThePeers()
	{
		[$p] = $this->openNode('P');
		[$q] = $this->openNode('Q');
		[$leaving] = $this->makeConnection();
		[$staying] = $this->makeConnection();
		$leavingId = $q->register($leaving);
		$stayingId = $q->register($staying);
		$p->tick();
		self::assertArrayHasKey($leavingId, $p->presence());

		$this->redis->failing = true;
		$q->tick();                  // Q faults and disconnects
		$q->unregister($leaving);    // the drop is lost: Redis is down
		$this->redis->failing = false;
		$this->mono->setSeconds(1000.0 + TRedisBackplane::RECONNECT_INTERVAL + 1);
		$q->tick();                  // Q reconnects and replaces its presence
		$p->tick();

		self::assertArrayNotHasKey($leavingId, $p->presence(), "The client that left during the outage leaves the peer's mirror.");
		self::assertArrayHasKey($stayingId, $p->presence(), 'The client still connected stays present on the peer.');
	}

	public function testARestartUnderTheSameNodeIdReplacesItsPresenceOnThePeers()
	{
		[$x] = $this->openNode('X');
		[$y1] = $this->openNode('Y');
		[$connection] = $this->makeConnection();
		$oldId = $y1->register($connection);
		$x->tick();
		self::assertArrayHasKey($oldId, $x->presence());

		[$y2] = $this->openNode('Y');   // Y crashed and restarted at once, inside its TTL
		[$fresh] = $this->makeConnection();
		$newId = $y2->register($fresh);
		$x->tick();

		self::assertArrayNotHasKey($oldId, $x->presence(), "The previous incarnation's client leaves the peer's mirror.");
		self::assertArrayHasKey($newId, $x->presence(), "The new incarnation's client is present.");
	}

	public function testANodeDownNeverDropsTheReceiversOwnClients()
	{
		[$n1] = $this->openNode('n1');
		[$connection] = $this->makeConnection();
		$clientId = $n1->register($connection);
		$this->redis->lists[$this->prefix . 'inbox:n1'][] = (new TWebSocketEnvelope(TWebSocketEnvelope::NODE_DOWN, 'n2', '', null, null, ['node' => 'n1']))->encode();

		$n1->tick();

		self::assertArrayHasKey($clientId, $n1->presence(), 'A node-down naming this node leaves its own clients alone.');
	}

	public function testCloseLeavesTheClusterAndReleasesTheConnection()
	{
		[$cluster, $backplane] = $this->openNode('n1');
		[$connection] = $this->makeConnection();
		$clientId = $cluster->register($connection);
		$cluster->subscribe($clientId, 'news');
		$this->redis->lists[$this->prefix . 'inbox:n1'][] = 'pending';

		$backplane->close();

		self::assertArrayNotHasKey('n1', $this->redis->sets[$this->prefix . 'nodes']);
		self::assertArrayNotHasKey('n1', $this->redis->sets[$this->prefix . 'ch:news'] ?? []);
		self::assertArrayNotHasKey($this->prefix . 'nodech:n1', $this->redis->sets);
		self::assertArrayNotHasKey($this->prefix . 'node:n1', $this->redis->strings);
		self::assertArrayNotHasKey($this->prefix . 'inbox:n1', $this->redis->lists);
		self::assertSame(1, $this->redis->closes, 'The connection is closed.');

		$backplane->close();   // already closed: a no-op
		self::assertSame(1, $this->redis->closes);
	}

	public function testCloseSwallowsAFaultFromADeadConnection()
	{
		[, $backplane] = $this->openNode('n1');
		$this->redis->failing = true;

		$backplane->close();   // the commands fail; the keys expire by TTL instead

		self::assertSame(0, $this->redis->closes);
		$this->redis->failing = false;
		$backplane->close();
		self::assertSame(0, $this->redis->closes, 'The backplane forgot the dead connection.');
	}

	public function testUnsubscribeWithdrawsChannelInterest()
	{
		[$cluster] = $this->openNode('n1');
		[$connection] = $this->makeConnection();
		$clientId = $cluster->register($connection);
		$cluster->subscribe($clientId, 'news');
		self::assertArrayHasKey('n1', $this->redis->sets[$this->prefix . 'ch:news']);

		$cluster->unsubscribe($clientId, 'news');

		self::assertArrayNotHasKey('n1', $this->redis->sets[$this->prefix . 'ch:news'] ?? []);
		self::assertArrayNotHasKey('news', $this->redis->sets[$this->prefix . 'nodech:n1'] ?? []);
	}

	public function testPublishReachesOnlyTheInterestedNodes()
	{
		[$n1] = $this->openNode('n1');
		[$n2] = $this->openNode('n2');
		$this->openNode('n3');
		[$connection] = $this->makeConnection();
		$n2->subscribe($n2->register($connection), 'news');

		$n1->publish('news', 'hello');
		$n1->publish('empty', 'nobody');   // no node is interested: nothing is pushed

		$publishes = fn (string $node) => array_map(fn ($e) => $e->getPayload(), array_values(array_filter($this->inbox($node), fn ($e) => $e->getType() === TWebSocketEnvelope::PUBLISH)));
		self::assertSame(['hello'], $publishes('n2'), 'The interested node receives the publish.');
		self::assertSame([], $publishes('n3'), 'A node without interest receives nothing.');
	}

	public function testDirectToAClientWithoutAResolvableNodeIsNotRouted()
	{
		[, $bp1] = $this->openNode('n1');
		$this->openNode('n2');
		$this->redis->hashes[$this->prefix . 'presence']['broken'] = '{not json';
		$this->redis->hashes[$this->prefix . 'presence']['nodeless'] = '{"user":"x"}';

		foreach (['broken', 'nodeless', 'unknown'] as $clientId) {
			$bp1->publish(new TWebSocketEnvelope(TWebSocketEnvelope::DIRECT, 'n1', 'lost', null, $clientId));
		}


		self::assertSame([], array_filter($this->inbox('n2'), fn ($e) => $e->getType() === TWebSocketEnvelope::DIRECT), 'A direct to a client with no resolvable node goes nowhere.');
		self::assertSame(2, $this->redis->connects, 'A malformed entry is not a Redis fault.');
	}

	public function testOpenAuthenticatesAndSelectsTheDatabase()
	{
		[$cluster, $backplane] = $this->makeNode('n1');
		$backplane->setPassword('secret');
		$backplane->setDatabase(3);
		$cluster->open();

		self::assertSame(['secret', 3], $this->redis->calls, 'The password and the database are sent on connect.');
	}

	public static function provideRefusedConnections(): array
	{
		return [
			'connect refused' => ['connectOk'],
			'auth refused' => ['authOk'],
			'select refused' => ['selectOk'],
		];
	}

	/**
	 * @dataProvider provideRefusedConnections
	 * @param string $flag
	 */
	public function testOpenFailsWhenTheServerRefusesTheConnection(string $flag)
	{
		$this->redis->{$flag} = false;
		[$cluster, $backplane] = $this->makeNode('n1');
		$backplane->setPassword('secret');
		$backplane->setDatabase(3);
		try {
			$cluster->open();
			self::fail('The open succeeded.');
		} catch (TConfigurationException $e) {
			self::assertSame('websocket_backplane_redis_connect_failed', $e->getErrorCode());
		}
		self::assertArrayNotHasKey($this->prefix . 'nodes', $this->redis->sets, 'Nothing is written through a refused connection.');
		$cluster->publish('news', 'dropped');   // the backplane holds no half-open handle
		self::assertSame([], $this->redis->lists);
	}

	public function testOpenWrapsAFaultInTheJoin()
	{
		[$cluster] = $this->makeNode('n1');
		$this->redis->failing = true;
		try {
			$cluster->open();
			self::fail('The open succeeded.');
		} catch (TConfigurationException $e) {
			self::assertSame('websocket_backplane_redis_connect_failed', $e->getErrorCode());
		}
	}

	public function testPropertiesRoundTripWithTheirDefaultsAndBounds()
	{
		$backplane = new TRedisBackplane();
		self::assertSame('127.0.0.1', $backplane->getHost());
		self::assertSame(6379, $backplane->getPort());
		self::assertNull($backplane->getPassword());
		self::assertSame(0, $backplane->getDatabase());
		self::assertSame('ws:', $backplane->getPrefix());
		self::assertSame(30, $backplane->getNodeTtl());
		self::assertSame(2.0, $backplane->getTimeout());
		self::assertSame(10000, $backplane->getInboxLimit());
		self::assertSame(10000, $backplane->getDrainLimit());
		self::assertSame($backplane, $backplane->setDrainLimit('25'));
		self::assertSame(25, $backplane->getDrainLimit());
		self::assertSame(0, $backplane->setDrainLimit(-1)->getDrainLimit(), 'A negative drain limit means a time bound only.');
		self::assertFalse($backplane->getHasBacklog(), 'A backplane that has not ticked has no backlog.');

		self::assertSame($backplane, $backplane->setHost('redis.internal')->setPort('6380')->setPassword('pw')->setDatabase('2')->setPrefix('app:')->setNodeTtl('12')->setTimeout('0.5')->setInboxLimit('50'));
		self::assertSame('redis.internal', $backplane->getHost());
		self::assertSame(6380, $backplane->getPort());
		self::assertSame('pw', $backplane->getPassword());
		self::assertSame(2, $backplane->getDatabase());
		self::assertSame('app:', $backplane->getPrefix());
		self::assertSame(12, $backplane->getNodeTtl());
		self::assertSame(0.5, $backplane->getTimeout());
		self::assertSame(50, $backplane->getInboxLimit());

		$backplane->setPassword('')->setNodeTtl(0)->setInboxLimit(-5);
		self::assertNull($backplane->getPassword(), 'An empty password means none.');
		self::assertSame(1, $backplane->getNodeTtl(), 'The TTL is at least one second.');
		self::assertSame(0, $backplane->getInboxLimit(), 'A negative limit means unlimited.');
		self::assertSame([], $backplane->getSources(), 'Redis is polled, not selected on.');
	}

	/** Queues $count publishes for 'news' on a node's inbox. */
	private function queuePublishes(string $node, int $count): void
	{
		for ($i = 0; $i < $count; $i++) {
			$this->redis->lists[$this->prefix . 'inbox:' . $node][] = (new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'n2', 'm' . $i, 'news'))->encode();
		}
	}

	public function testATickDrainsUpToTheDrainLimitAndReportsTheBacklog()
	{
		[$cluster, $backplane] = $this->openNode('n1');
		$backplane->setDrainLimit(10);
		$this->queuePublishes('n1', 25);
		$inbox = $this->prefix . 'inbox:n1';

		$cluster->tick();
		self::assertCount(15, $this->redis->lists[$inbox], 'One tick drains up to the drain limit.');
		self::assertTrue($backplane->getHasBacklog(), 'A tick that stops at the limit with envelopes left reports a backlog.');
		self::assertTrue($cluster->getHasBacklog(), 'The coordinator reports the backplane backlog to the server.');
		$cluster->tick();
		self::assertCount(5, $this->redis->lists[$inbox]);
		$cluster->tick();
		self::assertSame([], $this->redis->lists[$inbox], 'Successive ticks drain the rest.');
		self::assertFalse($backplane->getHasBacklog(), 'An emptied inbox leaves no backlog.');

		$backplane->setDrainLimit(0);
		$this->queuePublishes('n1', 2500);
		$cluster->tick();
		self::assertSame([], $this->redis->lists[$inbox], 'Without a count limit one tick reads every batch while time allows.');
		self::assertFalse($backplane->getHasBacklog());
	}

	public function testADrainStopsAtItsTimeBound()
	{
		[$cluster, $backplane] = $this->openNode('n1');
		$backplane->setDrainLimit(0);
		$slow = new class ($this->mono) extends \Prado\TComponent implements \Prado\IO\Socket\WebSocket\Cluster\IWebSocketCluster {
			public int $received = 0;

			public function __construct(private TTestMonotonicClock $mono)
			{
				parent::__construct();
			}

			public function getNodeId(): string
			{
				return 'n1';
			}

			public function receiveEnvelope(TWebSocketEnvelope $envelope): void
			{
				$this->received++;
				$this->mono->advance(0.001);   // each delivery takes a millisecond
			}

			public function hasLocalClient(string $clientId): bool
			{
				return false;
			}

			public function dropNodePresence(string $node): void
			{
			}

			public function getLocalPresence(): array
			{
				return [];
			}
		};
		$backplane->setCluster($slow);
		$this->queuePublishes('n1', 2500);

		$backplane->tick();

		self::assertSame(TRedisBackplane::DRAIN_LIMIT, $slow->received, 'The drain stops after the batch that passed DRAIN_TIME.');
		self::assertTrue($backplane->getHasBacklog(), 'The rest is left for the next tick, which the server runs at once.');
		self::assertCount(2500 - TRedisBackplane::DRAIN_LIMIT, $this->redis->lists[$this->prefix . 'inbox:n1']);
	}

	public function testADrainRacingAProducersTrimLosesNothing()
	{
		[$consumer] = $this->openNode('n1');
		[$producer, $bp2] = $this->openNode('n2');
		$bp2->setInboxLimit(3);
		[$connection, $sink] = $this->makeConnection();
		$consumer->subscribe($consumer->register($connection), 'news');
		$producer->tick();
		$inbox = $this->prefix . 'inbox:n1';
		$this->redis->lists[$inbox] = [];
		foreach (['a', 'b', 'c'] as $payload) {
			$producer->broadcast($payload);   // the inbox is at the producer's limit
		}

		// The producer pushes (and trims to its limit) while the consumer is reading its batch.
		$this->redis->after('lRange', [$inbox], fn () => $producer->broadcast('d'));
		$consumer->tick();
		$consumer->tick();

		foreach (['a', 'b', 'c', 'd'] as $payload) {
			self::assertStringContainsString($payload, (string) $sink, "Envelope {$payload} is delivered: the read and its removal are one transaction.");
		}
	}

	public function testAReapRacingTheNodesRejoinKeepsItsClientsOnEveryNode()
	{
		[$n1, $bp1] = $this->openNode('n1');
		[$n2] = $this->openNode('n2');
		[$n3] = $this->openNode('n3');
		[$connection] = $this->makeConnection();
		$clientId = $n1->register($connection);
		$n2->tick();
		$n3->tick();
		self::assertArrayHasKey($clientId, $n3->presence());

		// n1 stalls past the TTL; n2 reaps it, and n1 resumes just after the reap removes it from the registry.
		unset($this->redis->strings[$this->prefix . 'node:n1']);
		$this->mono->setSeconds(1000.0 + $bp1->getNodeTtl() + 1);
		$this->redis->after('sRem', [$this->prefix . 'nodes', 'n1'], fn () => $n1->tick());
		$n2->tick();
		$n2->tick();
		$n3->tick();

		self::assertArrayHasKey('n1', $this->redis->sets[$this->prefix . 'nodes'], 'The resumed node is in the registry.');
		self::assertArrayHasKey($clientId, $this->redis->hashes[$this->prefix . 'presence'], "The resumed node's client stays in the presence hash.");
		self::assertArrayHasKey($clientId, $n3->presence(), 'A third node sees the node-down before the re-announce, never after it.');
		self::assertArrayHasKey($clientId, $n2->presence(), 'The reaping node learns the client again.');
	}

	public function testAHeartbeatDuringAReapAbortsIt()
	{
		[$n1, $bp1] = $this->openNode('n1');
		[$n2] = $this->openNode('n2');
		[$n3] = $this->openNode('n3');
		[$connection] = $this->makeConnection();
		$clientId = $n1->register($connection);
		$n2->tick();
		$n3->tick();

		// n2 finds n1's heartbeat expired, and n1 heartbeats before n2's reap commits.
		unset($this->redis->strings[$this->prefix . 'node:n1']);
		$this->mono->setSeconds(1000.0 + $bp1->getNodeTtl() + 1);
		$this->redis->after('exists', [$this->prefix . 'node:n1'], fn () => $n1->tick());
		$n2->tick();
		$n3->tick();

		self::assertArrayHasKey('n1', $this->redis->sets[$this->prefix . 'nodes'], 'The live node is not reaped.');
		self::assertArrayHasKey($clientId, $this->redis->hashes[$this->prefix . 'presence']);
		self::assertArrayHasKey($clientId, $n2->presence(), 'The node that started the reap keeps the client.');
		self::assertArrayHasKey($clientId, $n3->presence(), 'No node-down reaches the other nodes.');
	}

	public function testAnInboxAPushRecreatesAfterItsNodeWasReapedExpires()
	{
		[$n1, $bp1] = $this->openNode('n1');
		[$n2] = $this->openNode('n2');
		[$n3] = $this->openNode('n3');
		[$connection] = $this->makeConnection();
		$n2->subscribe($n2->register($connection), 'news');

		// n2 dies; n3 reads the channel's interested nodes, then n1 reaps n2 before n3 pushes.
		unset($this->redis->strings[$this->prefix . 'node:n2']);
		$this->mono->setSeconds(1000.0 + $bp1->getNodeTtl() + 1);
		$this->redis->after('sMembers', [$this->prefix . 'ch:news'], fn () => $n1->tick());
		$n3->publish('news', 'late');

		$orphan = $this->prefix . 'inbox:n2';
		self::assertArrayNotHasKey('n2', $this->redis->sets[$this->prefix . 'nodes'], 'The dead node was reaped.');
		self::assertNotEmpty($this->redis->lists[$orphan] ?? [], 'The racing push recreated the reaped inbox.');
		self::assertSame(3 * $bp1->getNodeTtl(), $this->redis->ttls[$orphan] ?? null, 'The recreated inbox expires.');
	}

	public function testAWallClockStepDoesNotHastenAReconnect()
	{
		[$cluster] = $this->openNode('n1');
		$this->redis->failing = true;
		$cluster->tick();   // the fault disconnects; a reconnect is due RECONNECT_INTERVAL later
		$this->redis->failing = false;

		$this->clock->setMicrotime(1000.0 + 3600);   // the wall clock steps forward an hour
		$cluster->tick();
		self::assertSame(1, $this->redis->connects, 'A wall-clock step does not end the reconnect delay.');

		$this->mono->advance(TRedisBackplane::RECONNECT_INTERVAL + 1);
		$cluster->tick();
		self::assertSame(2, $this->redis->connects, 'The delay ends on the monotonic clock.');
	}

	public function testOpenRequiresTheRedisExtension()
	{
		$backplane = new NoExtensionRedisBackplane();
		new TWebSocketCluster('n1', $backplane);
		try {
			$backplane->open();
			self::fail('The open succeeded without ext-redis.');
		} catch (TConfigurationException $e) {
			self::assertSame('websocket_backplane_redis_missing', $e->getErrorCode());
		}
	}

	public function testAFailedLocalDeliveryDoesNotDisconnectOrDropTheBatch()
	{
		[$cluster] = $this->makeNode('n1');
		$cluster->open();
		$dead = new TWebSocketConnection(new ThrowingBufferStream(), false);
		[$live, $sink] = $this->makeConnection();
		$deadId = $cluster->register($dead);
		$liveId = $cluster->register($live);
		$cluster->subscribe($deadId, 'news');
		$cluster->subscribe($liveId, 'news');
		$inbox = $this->prefix . 'inbox:n1';
		$this->redis->lists[$inbox][] = (new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'n2', 'first', 'news'))->encode();
		$this->redis->lists[$inbox][] = (new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'n2', 'second', 'news'))->encode();

		$cluster->tick();

		self::assertSame(1, $this->redis->connects, 'A client send failure is not treated as a Redis fault.');
		self::assertStringContainsString('second', (string) $sink, 'The envelope after the failing delivery still reaches the live client.');
		self::assertArrayHasKey($this->prefix . 'node:n1', $this->redis->strings, 'The backplane stays connected and keeps heartbeating.');
	}
}
