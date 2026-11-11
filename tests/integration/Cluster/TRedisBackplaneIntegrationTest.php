<?php

namespace Prado\Test\Integration\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TConfigurationException;
use Prado\IO\Socket\WebSocket\Cluster\TRedisBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Stream\TBufferStream;
use Prado\Test\Unit\IO\Socket\WebSocket\TTestMonotonicClock;

/** A backplane that keeps the phpredis client it connects, so a test can read the options set on it. */
class CapturingRedisBackplane extends TRedisBackplane
{
	/** @var ?\Redis The client the last {@see open()} created. */
	public ?object $client = null;

	protected function createRedis(): object
	{
		return $this->client = parent::createRedis();
	}
}

/**
 * {@see TRedisBackplane} against a real Redis server through ext-redis: the commands, transactions
 * and pipelines the unit tests run against a hand-written fake.
 *
 * The server is read from the environment: `REDIS_HOST` (default 127.0.0.1) and `REDIS_PORT`
 * (default 6379).  Every test uses its own key prefix and deletes its keys.  The tests are skipped
 * when ext-redis is not loaded or the server cannot be reached.
 */
class TRedisBackplaneIntegrationTest extends TestCase
{
	private string $host;
	private int $port;
	private string $prefix;

	/** @var \Redis A client of the test's own, to inspect and prepare keys. */
	private object $redis;

	private TTestMonotonicClock $mono;

	/** @var TWebSocketCluster[] The nodes to close. */
	private array $nodes = [];

	protected function setUp(): void
	{
		if (!extension_loaded('redis')) {
			self::markTestSkipped('ext-redis is not loaded.');
		}
		$this->host = (string) (getenv('REDIS_HOST') ?: '127.0.0.1');
		$this->port = (int) (getenv('REDIS_PORT') ?: 6379);
		$this->redis = new \Redis();
		try {
			$connected = @$this->redis->connect($this->host, $this->port, 2.0);
		} catch (\Throwable $e) {
			$connected = false;
		}
		if (!$connected) {
			self::markTestSkipped("No Redis server answers at {$this->host}:{$this->port} (REDIS_HOST, REDIS_PORT).");
		}
		$this->prefix = 'wsit:' . bin2hex(random_bytes(6)) . ':';
		$this->mono = new TTestMonotonicClock(1000.0);
	}

	protected function tearDown(): void
	{
		foreach ($this->nodes as $node) {
			try {
				$node->close();
			} catch (\Throwable $e) {
				// A node a test disconnected has nothing to close.
			}
		}
		$this->nodes = [];
		if (isset($this->redis, $this->prefix) && $this->redis->isConnected()) {
			$keys = $this->redis->keys($this->prefix . '*');   // the prefix is this test's own
			if (is_array($keys) && $keys !== []) {
				$this->redis->del($keys);
			}
			$this->redis->close();
		}
	}

	/**
	 * Builds a cluster node on the real Redis, not yet opened.
	 * @param string $nodeId The node id.
	 * @return array{0: TWebSocketCluster, 1: CapturingRedisBackplane} The coordinator and its backplane.
	 */
	private function makeNode(string $nodeId): array
	{
		$backplane = new CapturingRedisBackplane();
		$backplane->setHost($this->host);
		$backplane->setPort($this->port);
		$backplane->setPrefix($this->prefix);
		$backplane->setMonotonicClock($this->mono);
		$this->nodes[] = $cluster = new TWebSocketCluster($nodeId, $backplane);
		return [$cluster, $backplane];
	}

	/**
	 * Builds and opens a cluster node.
	 * @param string $nodeId The node id.
	 * @return array{0: TWebSocketCluster, 1: CapturingRedisBackplane} The coordinator and its backplane.
	 */
	private function openNode(string $nodeId): array
	{
		[$cluster, $backplane] = $this->makeNode($nodeId);
		$cluster->open();
		return [$cluster, $backplane];
	}

	/** @return array{0: TWebSocketConnection, 1: TBufferStream} A server-side connection and its sink. */
	private function makeConnection(): array
	{
		$stream = new TBufferStream();
		return [new TWebSocketConnection($stream, false), $stream];
	}

	private function key(string $name): string
	{
		return $this->prefix . $name;
	}

	public function testOpenJoinsTheRegistryAndCloseLeavesIt()
	{
		[$cluster, $backplane] = $this->makeNode('n1');
		$backplane->setNodeTtl(30);
		$cluster->open();

		self::assertTrue($this->redis->sIsMember($this->key('nodes'), 'n1'), 'The node joins the registry.');
		$ttl = $this->redis->ttl($this->key('node:n1'));
		self::assertGreaterThan(0, $ttl, 'Its heartbeat key expires.');
		self::assertLessThanOrEqual(30, $ttl, 'After at most the node TTL.');

		$cluster->close();
		self::assertFalse($this->redis->sIsMember($this->key('nodes'), 'n1'), 'Closing leaves the registry.');
		self::assertSame(0, (int) $this->redis->exists($this->key('node:n1')), 'And removes the heartbeat.');
		self::assertSame(0, (int) $this->redis->exists($this->key('inbox:n1')));
	}

	public function testOpenFailsWhereNoServerAnswers()
	{
		$backplane = new TRedisBackplane();
		$backplane->setHost($this->host);
		$backplane->setPort(1);   // nothing listens on the tcpmux port
		$backplane->setTimeout(0.5);
		$cluster = new TWebSocketCluster('n1', $backplane);
		try {
			$cluster->open();
			self::fail('A refused connection is a configuration error.');
		} catch (TConfigurationException $e) {
			self::assertSame('websocket_backplane_redis_connect_failed', $e->getErrorCode());
		}
	}

	public function testTrafficIsPublishedByOneBackplaneAndDrainedByAnother()
	{
		[$n1] = $this->openNode('n1');
		[$n2] = $this->openNode('n2');
		[$subscriber, $subscriberSink] = $this->makeConnection();
		[$other, $otherSink] = $this->makeConnection();
		$subscriberId = $n1->register($subscriber);
		$n1->register($other);
		$n1->subscribe($subscriberId, 'news');
		self::assertTrue($this->redis->sIsMember($this->key('ch:news'), 'n1'), 'The node declares its channel interest.');

		$n2->tick();   // n2 learns n1's clients
		$n1->tick();   // n1 drains what n2's join queued
		$n2->publish('news', 'to the channel');
		$n2->publish('sports', 'to nobody');
		$n2->broadcast("binary \xff\x00 payload", true);
		self::assertTrue($n2->sendToClient($subscriberId, 'to one client'));
		self::assertSame(3, $this->redis->lLen($this->key('inbox:n1')), 'Only the envelopes that concern n1 are queued for it.');

		$n1->tick();
		self::assertSame(0, $this->redis->lLen($this->key('inbox:n1')), 'A tick drains the inbox.');
		self::assertStringContainsString('to the channel', (string) $subscriberSink);
		self::assertStringContainsString('to one client', (string) $subscriberSink);
		self::assertStringContainsString("binary \xff\x00 payload", (string) $subscriberSink, 'A binary payload crosses Redis intact.');
		self::assertStringNotContainsString('to the channel', (string) $otherSink, 'A publish reaches the subscriber only.');
		self::assertStringContainsString("binary \xff\x00 payload", (string) $otherSink, 'A broadcast reaches every client.');
		self::assertStringNotContainsString('to nobody', (string) $subscriberSink . (string) $otherSink);

		$n1->unsubscribe($subscriberId, 'news');
		self::assertFalse($this->redis->sIsMember($this->key('ch:news'), 'n1'), 'The last unsubscribe withdraws the interest.');
	}

	public function testPresenceIsSetDroppedAndSeeded()
	{
		[$n1] = $this->openNode('n1');
		[$n2] = $this->openNode('n2');
		[$connection] = $this->makeConnection();
		$clientId = $n1->register($connection, ['user' => 'alice', 'name' => "caf\xc3\xa9"]);

		$stored = json_decode((string) $this->redis->hGet($this->key('presence'), $clientId), true);
		self::assertSame(['user' => 'alice', 'name' => 'café', 'node' => 'n1'], $stored, 'The presence hash holds the client.');
		$n2->tick();
		self::assertSame('alice', $n2->presence()[$clientId]['user'] ?? null, 'The other node mirrors it.');

		[$n3] = $this->openNode('n3');   // a late joiner seeds from the hash
		self::assertSame('alice', $n3->presence()[$clientId]['user'] ?? null);

		$n1->unregister($clientId);
		self::assertFalse($this->redis->hExists($this->key('presence'), $clientId), 'The departure leaves the hash.');
		$n2->tick();
		$n3->tick();
		self::assertArrayNotHasKey($clientId, $n2->presence());
		self::assertArrayNotHasKey($clientId, $n3->presence());
	}

	public function testADeadNodeIsReapedOnceItsHeartbeatExpires()
	{
		[$n1, $bp1] = $this->makeNode('n1');
		[$n2, $bp2] = $this->makeNode('n2');
		[$n3] = $this->makeNode('n3');
		$bp1->setNodeTtl(30);
		$bp2->setNodeTtl(1);   // n2's heartbeat lives one second on the server's clock
		$n1->open();
		$n2->open();
		$n3->open();
		[$connection] = $this->makeConnection();
		$clientId = $n2->register($connection);
		$n2->subscribe($clientId, 'news');
		$n1->tick();
		$n3->tick();
		self::assertArrayHasKey($clientId, $n1->presence());

		usleep(1_300_000);   // n2 dies: it never ticks again, and Redis expires its heartbeat
		self::assertSame(0, (int) $this->redis->exists($this->key('node:n2')), 'The heartbeat of a node that stopped expires.');
		$this->mono->advance(31);   // n1's prune is due
		$n1->tick();

		self::assertFalse($this->redis->sIsMember($this->key('nodes'), 'n2'), 'The dead node leaves the registry.');
		self::assertTrue($this->redis->sIsMember($this->key('nodes'), 'n1'));
		self::assertFalse($this->redis->hExists($this->key('presence'), $clientId), 'Its clients leave the presence hash.');
		self::assertFalse($this->redis->sIsMember($this->key('ch:news'), 'n2'), 'Its channel interest is withdrawn.');
		self::assertSame(0, (int) $this->redis->exists($this->key('inbox:n2')), 'Its inbox is deleted.');
		self::assertArrayNotHasKey($clientId, $n1->presence(), 'The reaping node forgets its clients.');
		$n3->tick();
		self::assertArrayNotHasKey($clientId, $n3->presence(), 'So does every other node, from the node-down it is sent.');
	}

	public function testANodeReapedWhileStalledRejoinsAndReplacesItsMirror()
	{
		[$a] = $this->openNode('A');
		[$b] = $this->openNode('B');
		[$own] = $this->makeConnection();
		$local = $a->register($own);
		[$first] = $this->makeConnection();
		[$second] = $this->makeConnection();
		$gone = $b->register($first);
		$stays = $b->register($second);
		$a->tick();
		$b->tick();
		self::assertArrayHasKey($gone, $a->presence());

		$this->redis->del($this->key('node:A'));   // A stalls: its heartbeat expires
		$this->mono->advance(31);
		$b->tick();   // B reaps A in a WATCHed transaction
		self::assertFalse($this->redis->sIsMember($this->key('nodes'), 'A'));
		self::assertArrayNotHasKey($local, $b->presence());
		$b->unregister($gone);   // nothing is queued for A, which has no inbox

		$this->mono->advance(11);
		$a->tick();   // A resumes: its heartbeat finds it reaped, and it rejoins
		self::assertTrue($this->redis->sIsMember($this->key('nodes'), 'A'), 'The node rejoins the registry.');
		self::assertTrue($this->redis->hExists($this->key('presence'), $local), 'And declares its clients again.');
		self::assertArrayNotHasKey($gone, $a->presence(), 'It drops the remote client that left while it was reaped.');
		self::assertArrayHasKey($stays, $a->presence());
		self::assertArrayHasKey($local, $a->presence());
		$b->tick();
		self::assertArrayHasKey($local, $b->presence(), 'The other node learns its clients again.');
	}

	public function testATickDrainsUpToTheDrainLimitAndReportsTheBacklog()
	{
		[$n1, $bp1] = $this->openNode('n1');
		[$n2] = $this->openNode('n2');
		[, $sink] = $registered = $this->makeConnection();
		$n1->register($registered[0]);
		$n1->tick();   // drains what n2's join queued
		$bp1->setDrainLimit(10);
		for ($i = 0; $i < 25; $i++) {
			$n2->broadcast("message-{$i};");
		}
		self::assertSame(25, $this->redis->lLen($this->key('inbox:n1')));
		$ttl = $this->redis->ttl($this->key('inbox:n1'));
		self::assertGreaterThan(0, $ttl, 'An inbox expires, so one recreated after its node was reaped is removed.');
		self::assertLessThanOrEqual(3 * $bp1->getNodeTtl(), $ttl);

		$n1->tick();
		self::assertSame(15, $this->redis->lLen($this->key('inbox:n1')), 'One tick drains up to the drain limit, in one transaction.');
		self::assertTrue($n1->getHasBacklog(), 'A tick that stops at the limit reports the backlog.');
		self::assertSame(10, substr_count((string) $sink, 'message-'));
		$n1->tick();
		self::assertTrue($n1->getHasBacklog());
		$n1->tick();
		self::assertSame(0, $this->redis->lLen($this->key('inbox:n1')));
		self::assertFalse($n1->getHasBacklog(), 'An emptied inbox leaves none.');
		self::assertSame(25, substr_count((string) $sink, 'message-'), 'Every envelope is delivered once.');
		self::assertStringContainsString('message-0;', (string) $sink);
		self::assertStringContainsString('message-24;', (string) $sink);
		self::assertLessThan(strpos((string) $sink, 'message-24;'), strpos((string) $sink, 'message-0;'), 'In the order they were queued.');
	}

	public function testAnInboxKeepsItsNewestEntriesUpToTheInboxLimit()
	{
		[$n1] = $this->openNode('n1');
		[$n2, $bp2] = $this->openNode('n2');
		$bp2->setInboxLimit(3);
		for ($i = 0; $i < 8; $i++) {
			$n2->broadcast("message-{$i}");
		}
		$queued = $this->redis->lRange($this->key('inbox:n1'), 0, -1);
		self::assertCount(3, $queued, 'A push trims the inbox to the limit.');
		self::assertStringContainsString('message-5', $queued[0]);
		self::assertStringContainsString('message-7', $queued[2], 'The newest entries are kept.');
		unset($n1);
	}

	public function testTheReadTimeoutBoundsAStalledServer()
	{
		[$n1, $bp1] = $this->makeNode('n1');
		$bp1->setTimeout(0.2);
		$n1->open();
		self::assertEqualsWithDelta(0.2, (float) $bp1->client->getOption(\Redis::OPT_READ_TIMEOUT), 0.001, 'The connection reads under the Timeout.');
		[$n2] = $this->openNode('n2');
		$n1->tick();   // drains what n2's join queued
		$n2->broadcast('queued');

		self::assertNotFalse($this->redis->rawCommand('CLIENT', 'PAUSE', '1500'), 'The server stops answering for a while.');
		$started = hrtime(true);
		$n1->tick();   // the drain's read times out
		$seconds = (hrtime(true) - $started) / 1_000_000_000;
		self::assertLessThan(1.2, $seconds, 'A stalled server holds the tick for the Timeout, not for as long as it stalls.');
		self::assertFalse($n1->getHasBacklog(), 'The backplane is disconnected and reports no backlog.');

		usleep(1_600_000);   // the server answers again
		self::assertSame(1, $this->redis->lLen($this->key('inbox:n1')), 'The envelope waits in the inbox.');
		$n1->tick();
		self::assertSame(1, $this->redis->lLen($this->key('inbox:n1')), 'The reconnect waits for its interval.');
		$this->mono->advance(TRedisBackplane::RECONNECT_INTERVAL);
		$n1->tick();
		self::assertSame(0, $this->redis->lLen($this->key('inbox:n1')), 'The backplane reconnects and drains.');
		self::assertTrue($this->redis->sIsMember($this->key('nodes'), 'n1'));
	}

	public function testTheDatabaseIndexIsSelected()
	{
		[$cluster, $backplane] = $this->makeNode('n1');
		$backplane->setDatabase(1);
		$cluster->open();
		self::assertFalse($this->redis->sIsMember($this->key('nodes'), 'n1'), 'Nothing is written to database 0.');
		$this->redis->select(1);
		try {
			self::assertTrue($this->redis->sIsMember($this->key('nodes'), 'n1'), 'The node registers in the database it was given.');
			$cluster->close();
			self::assertSame(0, (int) $this->redis->exists($this->key('node:n1')));
		} finally {
			$this->redis->del($this->key('nodes'), $this->key('node:n1'), $this->key('nodech:n1'), $this->key('inbox:n1'), $this->key('presence'));
			$this->redis->select(0);
		}
	}
}
