<?php

use Prado\Exceptions\TConfigurationException;
use Prado\IO\Socket\WebSocket\Cluster\TFileBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Stream\TBufferStream;

class TFileBackplaneTest extends PHPUnit\Framework\TestCase
{
	private string $dir;

	protected function setUp(): void
	{
		$this->dir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'wscluster_' . uniqid('', true);
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
