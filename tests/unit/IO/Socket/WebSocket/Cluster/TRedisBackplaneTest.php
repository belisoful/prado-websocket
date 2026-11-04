<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TConfigurationException;
use Prado\IO\Socket\WebSocket\Cluster\TRedisBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Stream\TBufferStream;
use Prado\Util\Clock\TMockClock;

/** An in-memory stand-in for the phpredis client, covering the commands the backplane issues. */
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

	/** Forgets everything, as a restarted Redis would. */
	public function flush(): void
	{
		$this->sets = $this->hashes = $this->lists = $this->strings = [];
	}

	private function guard(): void
	{
		if ($this->failing) {
			throw new \RuntimeException('redis is down');
		}
	}

	public function connect(string $host, int $port, float $timeout): bool
	{
		$this->guard();
		$this->connects++;
		return $this->connectOk;
	}

	public function setOption(int $option, mixed $value): bool
	{
		return true;
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

	public function multi(int $mode = 1): static
	{
		$this->guard();
		return $this;
	}

	public function exec(): array
	{
		return [];
	}

	public function sAdd(string $key, string $member): int
	{
		$this->guard();
		$new = !isset($this->sets[$key][$member]);
		$this->sets[$key][$member] = true;
		return $new ? 1 : 0;
	}

	public function sRem(string $key, string $member): int
	{
		$this->guard();
		$had = isset($this->sets[$key][$member]);
		unset($this->sets[$key][$member]);
		return $had ? 1 : 0;
	}

	public function sMembers(string $key): array
	{
		$this->guard();
		return array_keys($this->sets[$key] ?? []);
	}

	public function setex(string $key, int $ttl, string $value): bool
	{
		$this->guard();
		$this->strings[$key] = $value;
		return true;
	}

	public function exists(string $key): int
	{
		$this->guard();
		return isset($this->strings[$key]) ? 1 : 0;
	}

	public function del(string $key): int
	{
		$this->guard();
		$had = isset($this->sets[$key]) || isset($this->hashes[$key]) || isset($this->lists[$key]) || isset($this->strings[$key]);
		unset($this->sets[$key], $this->hashes[$key], $this->lists[$key], $this->strings[$key]);
		return $had ? 1 : 0;
	}

	public function hSet(string $key, string $field, string $value): int
	{
		$this->guard();
		$new = !isset($this->hashes[$key][$field]);
		$this->hashes[$key][$field] = $value;
		return $new ? 1 : 0;
	}

	public function hDel(string $key, string $field): int
	{
		$this->guard();
		$had = isset($this->hashes[$key][$field]);
		unset($this->hashes[$key][$field]);
		return $had ? 1 : 0;
	}

	public function hGet(string $key, string $field): string|false
	{
		$this->guard();
		return $this->hashes[$key][$field] ?? false;
	}

	public function hGetAll(string $key): array
	{
		$this->guard();
		return $this->hashes[$key] ?? [];
	}

	public function rPush(string $key, string $value): int
	{
		$this->guard();
		$this->lists[$key][] = $value;
		return count($this->lists[$key]);
	}

	public function lRange(string $key, int $start, int $stop): array
	{
		$this->guard();
		$list = $this->lists[$key] ?? [];
		return array_slice($list, $start, $stop < 0 ? null : $stop - $start + 1);
	}

	public function lTrim(string $key, int $start, int $stop): bool
	{
		$this->guard();
		$list = $this->lists[$key] ?? [];
		$this->lists[$key] = array_slice($list, $start, $stop < 0 ? null : $stop - $start + 1);
		return true;
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
		return $this->fake;
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
	private string $prefix;

	protected function setUp(): void
	{
		$this->redis = new FakeRedis();
		$this->clock = new TMockClock();
		$this->clock->setMicrotime(1000.0);
		$this->prefix = (new TRedisBackplane())->getPrefix();
	}

	/** @return array{0: TWebSocketCluster, 1: FakeRedisBackplane} A cluster node on the fake Redis and its backplane. */
	private function makeNode(string $nodeId): array
	{
		$backplane = new FakeRedisBackplane($this->redis);
		$backplane->setClock($this->clock);
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
		$this->clock->setMicrotime(1000.0 + TRedisBackplane::RECONNECT_INTERVAL + 1);
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
		$this->clock->setMicrotime(1000.0 + TRedisBackplane::RECONNECT_INTERVAL + 1);
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
		$this->clock->setMicrotime(1000.0 + $bp1->getNodeTtl() + 1);
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
		$this->clock->setMicrotime(1000.0 + $bpC->getNodeTtl() + 1);
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
		$this->clock->setMicrotime(1000.0 + TRedisBackplane::RECONNECT_INTERVAL + 1);
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
		[$n1] = $this->openNode('n1');
		$this->openNode('n2');
		$this->redis->hashes[$this->prefix . 'presence']['broken'] = '{not json';
		$this->redis->hashes[$this->prefix . 'presence']['nodeless'] = '{"user":"x"}';

		$n1->sendToClient('broken', 'lost');
		$n1->sendToClient('nodeless', 'lost');
		$n1->sendToClient('unknown', 'lost');

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
