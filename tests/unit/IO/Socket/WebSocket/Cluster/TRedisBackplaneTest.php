<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
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
		return true;
	}

	public function setOption(int $option, mixed $value): bool
	{
		return true;
	}

	public function auth(string $password): bool
	{
		return true;
	}

	public function select(int $database): bool
	{
		return true;
	}

	public function close(): bool
	{
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
