<?php

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\IWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\TComponent;

/** A coordinator stand-in that records the envelopes a backplane delivers. */
class SpyCluster extends TComponent implements IWebSocketCluster
{
	public string $node;
	/** @var TWebSocketEnvelope[] */
	public array $received = [];

	public function __construct(string $node)
	{
		$this->node = $node;
		parent::__construct();
	}

	public function getNodeId(): string
	{
		return $this->node;
	}

	public function receiveEnvelope(TWebSocketEnvelope $envelope): void
	{
		$this->received[] = $envelope;
	}

	public function hasLocalClient(string $clientId): bool
	{
		return false;
	}
}

/** A mesh that records dial attempts instead of opening real sockets. */
class RecordingMesh extends TMeshBackplane
{
	/** @var string[] */
	public array $dialed = [];

	public function connectPeer(string $uri): void
	{
		$this->dialed[] = $uri;
	}
}

class TMeshBackplaneTest extends PHPUnit\Framework\TestCase
{
	/** @return array{0: TMeshBackplane, 1: SpyCluster} A mesh node and its spy coordinator. */
	private function makeNode(string $id): array
	{
		$mesh = new TMeshBackplane();
		$spy = new SpyCluster($id);
		$mesh->setCluster($spy);
		return [$mesh, $spy];
	}

	/** Links two mesh nodes with a connected socket pair (dialer is the client role). */
	private function link(TMeshBackplane $a, TMeshBackplane $b): void
	{
		[$rawA, $rawB] = TSocketStream::pair();
		$rawA->setBlocking(false);
		$rawB->setBlocking(false);
		$a->addPeer(new TWebSocketConnection($rawA, true), $rawA);
		$b->addPeer(new TWebSocketConnection($rawB, false), $rawB);
	}

	public function testPublishFloodsToTheLinkedPeer()
	{
		[$a, $spyA] = $this->makeNode('A');
		[$b, $spyB] = $this->makeNode('B');
		$this->link($a, $b);

		$a->publish(new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'A', 'hello', 'news'));
		$b->tick();

		self::assertCount(1, $spyB->received, 'The peer delivers the flooded envelope.');
		self::assertSame('hello', $spyB->received[0]->getPayload());
		self::assertSame('news', $spyB->received[0]->getChannel());
		self::assertCount(0, $spyA->received, 'A node does not deliver its own published envelope to itself.');
	}

	public function testBroadcastReachesThePeer()
	{
		[$a] = $this->makeNode('A');
		[$b, $spyB] = $this->makeNode('B');
		$this->link($a, $b);

		$a->publish(new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'A', 'all'));
		$b->tick();

		self::assertCount(1, $spyB->received);
		self::assertSame(TWebSocketEnvelope::BROADCAST, $spyB->received[0]->getType());
	}

	public function testDuplicateSuppressedAcrossATriangle()
	{
		[$a] = $this->makeNode('A');
		[$b, $spyB] = $this->makeNode('B');
		[$c, $spyC] = $this->makeNode('C');
		$this->link($a, $b);
		$this->link($a, $c);
		$this->link($b, $c);

		$a->publish(new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'A', 'once', 'news', null, [], 'env-1'));
		$b->tick();   // delivers, re-floods to C
		$c->tick();   // sees A's direct copy and B's re-flooded copy

		$copies = array_filter($spyC->received, fn ($e) => $e->getId() === 'env-1');
		self::assertCount(1, $copies, 'A duplicate arriving over a second path is dropped.');
		self::assertCount(1, $spyB->received);
	}

	public function testPresenceSnapshotConvergesANewPeer()
	{
		[$a] = $this->makeNode('A');
		[$b, $spyB] = $this->makeNode('B');
		$a->putPresence('A-1', ['node' => 'A', 'user' => 'alice']);   // a local client before any peer

		$this->link($a, $b);   // adding the peer sends A's presence snapshot
		$b->tick();

		$presence = array_filter($spyB->received, fn ($e) => $e->getType() === TWebSocketEnvelope::PRESENCE_SET);
		self::assertCount(1, $presence);
		$envelope = array_values($presence)[0];
		self::assertSame('A-1', $envelope->getClientId());
		self::assertSame('alice', $envelope->getMeta()['user']);
	}

	public function testDeadPeerIsPruned()
	{
		[$a] = $this->makeNode('A');
		[$b] = $this->makeNode('B');
		$this->link($a, $b);
		self::assertSame(1, $a->getPeerCount());

		$b->close();        // the far end goes away
		$a->tick();         // the read reports end of stream
		self::assertSame(0, $a->getPeerCount(), 'A closed peer link is pruned.');
	}

	/**
	 * Simulates a remote peer connecting and announcing itself ($remoteUri), then gossiping further
	 * node URIs, all over one fresh link, and pumps the mesh.
	 * @param string[] $gossiped The third-party URIs the remote gossips after its self-announce.
	 * @param TMeshBackplane $mesh
	 * @param string $remoteUri
	 */
	private function feedGossip(TMeshBackplane $mesh, string $remoteUri, array $gossiped = []): void
	{
		[$rawLocal, $rawRemote] = TSocketStream::pair();
		$rawLocal->setBlocking(false);
		$rawRemote->setBlocking(false);
		$mesh->addPeer(new TWebSocketConnection($rawLocal, false), $rawLocal);
		$remote = new TWebSocketConnection($rawRemote, true);
		$remote->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'remote', '', null, null, ['uri' => $remoteUri]))->encode());
		foreach ($gossiped as $uri) {
			$remote->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'src-' . $uri, '', null, null, ['uri' => $uri]))->encode());
		}
		$mesh->tick();
	}

	public function testAddPeerAnnouncesItsAdvertisedUri()
	{
		[$a] = $this->makeNode('A');
		$a->setAdvertise('tcp://node-a:9');
		[$b, $spyB] = $this->makeNode('B');
		$b->setAdvertise('tcp://node-b:9');   // sorts after node-a, so B will not dial A back
		$this->link($a, $b);
		$b->tick();

		$announces = array_filter($spyB->received, fn ($e) => $e->getType() === TWebSocketEnvelope::NODE_UP);
		self::assertNotEmpty($announces, 'A node announces its advertised URI to a new peer.');
		self::assertSame('tcp://node-a:9', array_values($announces)[0]->getMeta()['uri']);
	}

	public function testGossipDialsAnUnknownHigherSortingPeer()
	{
		$mesh = new RecordingMesh();
		$mesh->setAdvertise('tcp://node-b:1');
		$mesh->setCluster(new SpyCluster('B'));

		$this->feedGossip($mesh, 'tcp://remote:1', ['tcp://node-c:1']);
		self::assertContains('tcp://node-c:1', $mesh->dialed, 'A gossiped higher-sorting peer is dialed.');
		self::assertNotContains('tcp://remote:1', $mesh->dialed, 'The directly-connected peer is not re-dialed.');
	}

	public function testGossipSkipsSelfLowerAndDuplicates()
	{
		$mesh = new RecordingMesh();
		$mesh->setAdvertise('tcp://node-m:1');
		$mesh->setCluster(new SpyCluster('M'));

		$this->feedGossip($mesh, 'tcp://remote:1', [
			'tcp://node-m:1',   // self
			'tcp://node-a:1',   // lower-sorting: the other side dials us
			'tcp://node-z:1',   // higher-sorting: dial
			'tcp://node-z:1',   // duplicate
		]);
		self::assertSame(['tcp://node-z:1'], $mesh->dialed, 'A node dials only the new higher-sorting peer, once.');
	}

	public function testGossipDoesNotRedialAnAlreadyLinkedPeer()
	{
		$mesh = new RecordingMesh();
		$mesh->setAdvertise('tcp://node-a:1');
		$mesh->setCluster(new SpyCluster('A'));

		// The remote announces itself as node-x (binding the link), then gossips node-x again.
		$this->feedGossip($mesh, 'tcp://node-x:1', ['tcp://node-x:1']);
		self::assertSame([], $mesh->dialed, 'A peer already linked is not dialed again.');
	}

	public function testAuthenticateRequiresAValidProofWhenASecretIsSet()
	{
		$mesh = new TMeshBackplane();
		self::assertFalse($mesh->authenticate([]), 'A mesh without a secret is disabled and accepts no peer.');

		$mesh->setSecret('s3cr3t');
		$key = 'dGhlIHNhbXBsZSBub25jZQ==';
		$proof = base64_encode(hash_hmac('sha256', $key, sha1('s3cr3t'), true));
		self::assertTrue($mesh->authenticate(['sec-websocket-key' => $key, 'x-cluster-auth' => $proof]), 'A valid HMAC proof is accepted.');
		self::assertFalse($mesh->authenticate(['sec-websocket-key' => $key, 'x-cluster-auth' => 'forged']), 'A wrong proof is rejected.');
		self::assertFalse($mesh->authenticate(['sec-websocket-key' => $key]), 'A missing proof is rejected.');
		self::assertFalse($mesh->authenticate([]), 'A missing handshake key is rejected.');
	}
}
