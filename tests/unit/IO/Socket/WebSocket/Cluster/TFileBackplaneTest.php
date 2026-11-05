<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TConfigurationException;
use Prado\IO\Socket\WebSocket\Cluster\TFileBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Stream\TBufferStream;
use Prado\Prado;
use Prado\Test\Unit\IO\Socket\WebSocket\TTestMonotonicClock;
use Prado\Util\Clock\TMockClock;
use Prado\Util\Log\TLogger;

class TFileBackplaneTest extends TestCase
{
	private string $dir;
	private TTestMonotonicClock $mono;

	protected function setUp(): void
	{
		$this->dir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'wscluster_' . uniqid('', true);
		$this->mono = new TTestMonotonicClock(1000.0);
	}

	/** Moves the wall clock and the monotonic clock forward together, as time passing does. */
	private function advance(TMockClock $clock, float $seconds): void
	{
		$clock->setMicrotime($clock->microtime() + $seconds);
		$this->mono->advance($seconds);
	}

	protected function tearDown(): void
	{
		$this->removeTree($this->dir);
	}

	private function removeTree(string $dir): void
	{
		if (!is_dir($dir)) {
			return;
		}
		foreach (glob($dir . DIRECTORY_SEPARATOR . '*') ?: [] as $path) {
			is_dir($path) ? $this->removeTree($path) : @unlink($path);
		}
		@rmdir($dir);
	}

	private function makeNode(string $nodeId): TWebSocketCluster
	{
		$backplane = new TFileBackplane();
		$backplane->setDirectory($this->dir);
		$cluster = new TWebSocketCluster($nodeId, $backplane);
		$cluster->open();
		return $cluster;
	}

	/** @return array{0: TWebSocketConnection, 1: TBufferStream} A server-side connection and its sink. */
	private function makeConnection(): array
	{
		$stream = new TBufferStream();
		return [new TWebSocketConnection($stream, false), $stream];
	}

	/** Opens a node on the shared directory whose backplane reads the given clock. */
	private function makeClockedNode(string $nodeId, TMockClock $clock): TWebSocketCluster
	{
		$backplane = new TFileBackplane();
		$backplane->setDirectory($this->dir);
		$backplane->setClock($clock);
		$backplane->setMonotonicClock($this->mono);
		$cluster = new TWebSocketCluster($nodeId, $backplane);
		$cluster->open();
		return $cluster;
	}

	private function presenceFile(string $clientId): string
	{
		return $this->dir . DIRECTORY_SEPARATOR . 'presence' . DIRECTORY_SEPARATOR . rawurlencode($clientId);
	}

	public function testEveryNodeForgetsAClientWhoseNodeCrashed()
	{
		$clock = new TMockClock();
		$clock->setMicrotime((float) time());
		$a = $this->makeClockedNode('A', $clock);
		$b = $this->makeClockedNode('B', $clock);
		$c = $this->makeClockedNode('C', $clock);
		[$connection] = $this->makeConnection();
		$clientId = $c->register($connection);
		$a->tick();
		$b->tick();
		self::assertArrayHasKey($clientId, $a->presence());
		self::assertArrayHasKey($clientId, $b->presence());

		// C crashes: it stops refreshing its file.  Whichever node reaps the file first, both forget the client.
		for ($i = 0; $i < 20; $i++) {
			$this->advance($clock, 5);
			$a->tick();
			$b->tick();
		}

		self::assertFileDoesNotExist($this->presenceFile($clientId));
		self::assertArrayNotHasKey($clientId, $a->presence(), "A running node forgets the crashed node's client.");
		self::assertArrayNotHasKey($clientId, $b->presence(), "Every running node forgets the crashed node's client.");
	}

	public function testTheReaperDropsTheDecodedClientId()
	{
		$clock = new TMockClock();
		$clock->setMicrotime((float) time());
		$p = $this->makeClockedNode('P', $clock);
		$q = $this->makeClockedNode('host:q', $clock);   // a node id that the file name URL-encodes
		[$connection] = $this->makeConnection();
		$clientId = $q->register($connection);
		$p->tick();
		self::assertArrayHasKey($clientId, $p->presence());

		for ($i = 0; $i < 20; $i++) {
			$this->advance($clock, 5);
			$p->tick();   // Q has crashed
		}

		self::assertFileDoesNotExist($this->presenceFile($clientId));
		self::assertArrayNotHasKey($clientId, $p->presence(), 'The reaper drops the client id, not its encoded file name.');
	}

	public function testARestartUnderTheSameNodeIdReplacesItsPresence()
	{
		$clock = new TMockClock();
		$clock->setMicrotime((float) time());
		$x = $this->makeClockedNode('X', $clock);
		$y1 = $this->makeClockedNode('Y', $clock);
		[$connection] = $this->makeConnection();
		$oldId = $y1->register($connection);
		$x->tick();
		self::assertArrayHasKey($oldId, $x->presence());

		$y2 = $this->makeClockedNode('Y', $clock);   // Y crashed and restarted at once
		[$fresh] = $this->makeConnection();
		$newId = $y2->register($fresh);
		$x->tick();

		self::assertFileDoesNotExist($this->presenceFile($oldId), "The previous incarnation's presence file is removed.");
		self::assertArrayNotHasKey($oldId, $x->presence(), "The previous incarnation's client leaves the peer's mirror.");
		self::assertArrayHasKey($newId, $x->presence(), "The new incarnation's client is present.");
		self::assertArrayNotHasKey($oldId, $this->makeClockedNode('Z', $clock)->presence(), 'A late joiner never sees the previous incarnation.');
	}

	public function testOpenKeepsAndReannouncesClientsRegisteredBeforeIt()
	{
		$clock = new TMockClock();
		$clock->setMicrotime((float) time());
		$x = $this->makeClockedNode('X', $clock);
		$backplane = new TFileBackplane();
		$backplane->setDirectory($this->dir);
		$backplane->setClock($clock);
		$y = new TWebSocketCluster('Y', $backplane);
		[$connection] = $this->makeConnection();
		$clientId = $y->register($connection);   // registered before the backplane opens
		$y->open();
		$x->tick();

		self::assertFileExists($this->presenceFile($clientId), "Opening keeps a current client's file.");
		self::assertArrayHasKey($clientId, $x->presence(), 'The current client is announced after the presence reset.');
	}

	public function testAStalledNodeRewritesAndReannouncesAReapedPresenceFile()
	{
		$clock = new TMockClock();
		$clock->setMicrotime((float) time());
		$x = $this->makeClockedNode('X', $clock);
		$y = $this->makeClockedNode('Y', $clock);
		[$connection] = $this->makeConnection();
		$clientId = $y->register($connection);
		$x->tick();

		for ($i = 0; $i < 20; $i++) {
			$this->advance($clock, 5);
			$x->tick();   // Y stalls: X reaps its file and drops the client
		}
		self::assertArrayNotHasKey($clientId, $x->presence());
		self::assertFileDoesNotExist($this->presenceFile($clientId));

		$y->tick();   // Y resumes
		$x->tick();

		self::assertFileExists($this->presenceFile($clientId), 'The resumed node writes its live client\'s file again.');
		self::assertGreaterThan(0, filesize($this->presenceFile($clientId)), 'The rewritten file holds the metadata, not an empty touch.');
		self::assertArrayHasKey($clientId, $x->presence(), 'The resumed node announces its live client again.');
		self::assertArrayHasKey($clientId, $this->makeClockedNode('Z', $clock)->presence(), 'A late joiner sees the live client.');
	}

	public function testAWallClockStepDoesNotHastenThePresenceRefresh()
	{
		$clock = new TMockClock();
		$clock->setMicrotime((float) time());
		$x = $this->makeClockedNode('X', $clock);
		[$connection] = $this->makeConnection();
		$clientId = $x->register($connection);
		$x->tick();   // the first refresh
		$refreshed = filemtime($this->presenceFile($clientId));

		$clock->setMicrotime($clock->microtime() + 3600);   // the wall clock steps forward an hour
		$x->tick();
		clearstatcache();
		self::assertSame($refreshed, filemtime($this->presenceFile($clientId)), 'A wall-clock step does not bring the next refresh forward.');

		$this->mono->advance($x->getBackplane()->getPresenceTtl() / 3);
		$x->tick();
		clearstatcache();
		self::assertSame((int) $clock->time(), filemtime($this->presenceFile($clientId)), 'The refresh runs a third of the TTL later on the monotonic clock, stamping the wall time.');
	}

	public function testAResumedNodeNeverReapsItsOwnLiveClient()
	{
		$clock = new TMockClock();
		$clock->setMicrotime((float) time());
		$x = $this->makeClockedNode('X', $clock);
		$y = $this->makeClockedNode('Y', $clock);
		[$connection] = $this->makeConnection();
		$clientId = $x->register($connection);
		$x->tick();
		$y->tick();
		self::assertArrayHasKey($clientId, $y->presence());

		$this->advance($clock, 10 * $x->getBackplane()->getPresenceTtl());   // the whole host was suspended
		$x->tick();   // X resumes first: it refreshes its client's file and reaps what is stale
		$y->tick();

		self::assertFileExists($this->presenceFile($clientId), 'The refreshed file is not reaped by its own node.');
		self::assertArrayHasKey($clientId, $y->presence(), 'No drop of the live client reaches the other node.');
	}

	public function testATickReadsUpToTheDrainLimitAndReportsTheBacklog()
	{
		$node1 = $this->makeNode('node1');
		$node2 = $this->makeNode('node2');
		$backplane = $node2->getBackplane();
		[$connection, $sink] = $this->makeConnection();
		$node2->subscribe($node2->register($connection), 'news');
		$node2->tick();   // reads the node's own join entries
		$backplane->setDrainLimit(3);
		for ($i = 1; $i <= 7; $i++) {
			$node1->publish('news', 'm' . $i);
		}

		$node2->tick();
		self::assertStringContainsString('m3', (string) $sink);
		self::assertStringNotContainsString('m4', (string) $sink, 'One tick reads up to the drain limit.');
		self::assertTrue($backplane->getHasBacklog(), 'A tick that stops at the limit with entries left reports a backlog.');
		self::assertTrue($node2->getHasBacklog());
		$node2->tick();
		$node2->tick();
		self::assertStringContainsString('m7', (string) $sink, 'Successive ticks read the rest.');
		self::assertFalse($backplane->getHasBacklog(), 'A tick that reaches the end of the log leaves no backlog.');

		self::assertSame(10000, (new TFileBackplane())->getDrainLimit());
		self::assertSame(0, $backplane->setDrainLimit(-2)->getDrainLimit(), 'A negative limit means unlimited.');
		for ($i = 8; $i <= 20; $i++) {
			$node1->publish('news', 'm' . $i);
		}
		$node2->tick();
		self::assertStringContainsString('m20', (string) $sink, 'An unlimited tick reads the whole log.');
	}

	public function testAWritableParentDirectoryIsReported()
	{
		$parent = $this->dir . '_parent';
		mkdir($parent, 0o777, true);
		chmod($parent, 0o777);   // another user could rename the spool inside it
		$sticky = $this->dir . '_sticky';
		mkdir($sticky, 0o777, true);
		chmod($sticky, 0o1777);   // as /tmp: others can write, but cannot rename what they do not own
		$warnings = function (string $spool): array {
			Prado::getLogger()->deleteLogs(null, TFileBackplane::class);
			$backplane = new TFileBackplane();
			$backplane->setDirectory($spool);
			new TWebSocketCluster('n1', $backplane);
			$backplane->open();
			return array_filter(Prado::getLogger()->getLogs(TLogger::WARNING, TFileBackplane::class), fn ($log) => str_contains($log[0], 'unsafe parent'));
		};
		try {
			self::assertCount(1, $warnings($parent . DIRECTORY_SEPARATOR . 'spool'), 'A parent that others can write is reported.');
			self::assertCount(0, $warnings($sticky . DIRECTORY_SEPARATOR . 'spool'), 'A sticky parent is safe.');
			self::assertCount(0, $warnings($this->dir), 'The temporary directory is safe.');
		} finally {
			$this->removeTree($parent);
			$this->removeTree($sticky);
		}
	}

	public function testOpenRequiresADirectory()
	{
		$cluster = new TWebSocketCluster('lonely', new TFileBackplane());
		$this->expectException(TConfigurationException::class);
		$cluster->open();
	}

	public function testSpoolIsCreatedOwnerOnly()
	{
		$backplane = new TFileBackplane();
		$backplane->setDirectory($this->dir);
		new TWebSocketCluster('n1', $backplane);
		$backplane->open();
		self::assertSame(0o700, fileperms($this->dir . DIRECTORY_SEPARATOR . 'presence') & 0o777, 'The presence directory is owner-only, not 0777.');

		$backplane->putPresence('c1', ['node' => 'n1']);
		self::assertSame(0o600, fileperms($this->dir . DIRECTORY_SEPARATOR . 'presence' . DIRECTORY_SEPARATOR . 'c1') & 0o777, 'A presence file is owner-only, not world-readable.');
	}

	public function testOpenRefusesASymlinkedDirectory()
	{
		$real = $this->dir . '_real';
		mkdir($real, 0o700, true);
		symlink($real, $this->dir);   // an attacker redirects the spool through a symlink

		$backplane = new TFileBackplane();
		$backplane->setDirectory($this->dir);
		new TWebSocketCluster('n1', $backplane);
		try {
			$this->expectException(TConfigurationException::class);
			$backplane->open();
		} finally {
			@unlink($this->dir);
			$this->removeTree($real);
		}
	}

	public function testOpenRefusesAnOtherWritableDirectory()
	{
		mkdir($this->dir, 0o777, true);   // a pre-existing group/other-writable spool another user could tamper with
		chmod($this->dir, 0o777);

		$backplane = new TFileBackplane();
		$backplane->setDirectory($this->dir);
		new TWebSocketCluster('n1', $backplane);
		$this->expectException(TConfigurationException::class);
		$backplane->open();
	}

	public function testStalePresenceFilesAreReapedWhileFreshOnesSurvive()
	{
		$backplane = new TFileBackplane();
		$backplane->setDirectory($this->dir);
		new TWebSocketCluster('n1', $backplane);
		$backplane->open();
		$backplane->putPresence('live', ['node' => 'n1']);   // just written, fresh

		$ghost = $this->dir . DIRECTORY_SEPARATOR . 'presence' . DIRECTORY_SEPARATOR . 'ghost';
		file_put_contents($ghost, '{"node":"dead"}');
		touch($ghost, time() - 100);   // a crashed node's residue, well past 2x the default TTL

		$fresh = new TFileBackplane();
		$fresh->setDirectory($this->dir);
		new TWebSocketCluster('n2', $fresh);
		$fresh->open();   // reapStalePresence() runs before seeding

		self::assertFileDoesNotExist($ghost, "A crashed node's stale presence file is reaped, not replayed.");
		self::assertFileExists($this->dir . DIRECTORY_SEPARATOR . 'presence' . DIRECTORY_SEPARATOR . 'live', 'A recently written presence file survives.');
	}

	public function testARunningNodeForgetsACrashedNodesClientsWhenTheirPresenceGoesStale()
	{
		$clock = new TMockClock();
		$clock->setMicrotime(microtime(true));
		$backplane = new TFileBackplane();
		$backplane->setDirectory($this->dir);
		$backplane->setClock($clock);
		$cluster = new TWebSocketCluster('n1', $backplane);
		$cluster->open();

		// A remote node announced a client, then crashed without dropping it.
		$cluster->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'dead', '', null, 'ghost', ['node' => 'dead']));
		$ghost = $this->dir . DIRECTORY_SEPARATOR . 'presence' . DIRECTORY_SEPARATOR . 'ghost';
		file_put_contents($ghost, '{"node":"dead"}');
		touch($ghost, $clock->time());
		self::assertArrayHasKey('ghost', $cluster->presence());

		$clock->setMicrotime($clock->microtime() + 2 * $backplane->getPresenceTtl() + 5);   // nobody refreshed the file
		$cluster->tick();

		self::assertFileDoesNotExist($ghost, 'The stale presence file is reaped.');
		self::assertArrayNotHasKey('ghost', $cluster->presence(), 'The running node drops the crashed node\'s client from its mirror.');
	}

	public function testAGroupReadableSpoolIsRefused()
	{
		mkdir($this->dir, 0o750, true);   // group can list the spool: presence file names (client ids) would leak
		chmod($this->dir, 0o750);
		$backplane = new TFileBackplane();
		$backplane->setDirectory($this->dir);
		new TWebSocketCluster('n1', $backplane);
		$this->expectException(TConfigurationException::class);
		$backplane->open();
	}

	public function testAFailedLocalDeliveryDoesNotDropTheRestOfTheBatch()
	{
		$node1 = $this->makeNode('node1');
		$node2 = $this->makeNode('node2');
		$dead = new TWebSocketConnection(new ThrowingBufferStream(), false);
		[$live, $sink] = $this->makeConnection();
		$node2->subscribe($node2->register($dead), 'news');
		$node2->subscribe($node2->register($live), 'news');

		$node1->publish('news', 'first');
		$node1->publish('news', 'second');
		$node2->tick();

		self::assertStringContainsString('first', (string) $sink);
		self::assertStringContainsString('second', (string) $sink, 'The envelope after the failing delivery is still delivered.');
	}

	public function testPublishCrossesNodesThroughTheFiles()
	{
		$node1 = $this->makeNode('node1');
		$node2 = $this->makeNode('node2');
		[$conn, $sink] = $this->makeConnection();
		$id = $node2->register($conn);
		$node2->subscribe($id, 'news');

		$node1->publish('news', 'hello');
		self::assertSame(0, $sink->getSize(), 'Nothing is delivered until the other node ticks.');

		$node2->tick();
		self::assertGreaterThan(0, $sink->getSize(), 'A publish on node1 reaches a subscriber on node2.');
	}

	public function testTruncatedLogIsReReadFromStart()
	{
		$writer = $this->makeNode('w');
		$reader = $this->makeNode('r');
		[$conn, $sink] = $this->makeConnection();
		$reader->register($conn);

		$writer->broadcast('first');
		$writer->broadcast('second');
		$reader->tick();   // the reader's offset advances past two broadcasts
		$delivered = $sink->getSize();
		self::assertGreaterThan(0, $delivered);

		file_put_contents($this->dir . DIRECTORY_SEPARATOR . 'messages.log', '');   // the log is rotated to a smaller file
		$writer->broadcast('after');
		$reader->tick();   // the reader detects the shrink and re-reads from the start rather than seeking past EOF
		self::assertGreaterThan($delivered, $sink->getSize(), 'A truncated/rotated log is re-read from the start, not stranded.');
	}

	public function testBroadcastCrossesNodes()
	{
		$node1 = $this->makeNode('node1');
		$node2 = $this->makeNode('node2');
		[$conn, $sink] = $this->makeConnection();
		$node2->register($conn);

		$node1->broadcast('everyone');
		$node2->tick();
		self::assertGreaterThan(0, $sink->getSize(), 'A broadcast on node1 reaches every client on node2.');
	}

	public function testDirectMessageRoutesToTheHoldingNode()
	{
		$node1 = $this->makeNode('node1');
		$node2 = $this->makeNode('node2');
		[$conn, $sink] = $this->makeConnection();
		$remoteId = $node2->register($conn);

		$node1->tick();   // learn node2's client through the presence delta
		self::assertArrayHasKey($remoteId, $node1->presence(), 'Presence converges across nodes.');
		self::assertTrue($node1->sendToClient($remoteId, 'hi'), 'A remote client is known through the mirror.');

		$node2->tick();
		self::assertGreaterThan(0, $sink->getSize(), 'A direct send reaches the client on its node.');
	}

	public function testPresenceSeedsOnLateJoin()
	{
		$node1 = $this->makeNode('node1');
		[$conn] = $this->makeConnection();
		$id = $node1->register($conn, ['user' => 'alice']);

		$node2 = $this->makeNode('node2');   // joins after node1's client is present
		self::assertArrayHasKey($id, $node2->presence(), 'A late-joining node seeds presence from the registry.');
		self::assertSame('node1', $node2->presence()[$id]['node']);
		self::assertSame('alice', $node2->presence()[$id]['user']);
	}

	public function testPresenceDropPropagates()
	{
		$node1 = $this->makeNode('node1');
		$node2 = $this->makeNode('node2');
		[$conn] = $this->makeConnection();
		$id = $node1->register($conn);

		$node2->tick();
		self::assertArrayHasKey($id, $node2->presence());

		$node1->unregister($conn);
		$node2->tick();
		self::assertArrayNotHasKey($id, $node2->presence(), 'A departure propagates to the other node.');
	}
}
