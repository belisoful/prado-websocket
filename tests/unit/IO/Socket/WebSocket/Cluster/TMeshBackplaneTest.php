<?php

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\IWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
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

	/** @var string[] The nodes the backplane declared dead. */
	public array $droppedNodes = [];

	public function dropNodePresence(string $node): void
	{
		$this->droppedNodes[] = $node;
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

/** A mesh with a settable clock, so the failure detector's TTL can be advanced deterministically. */
class ClockMesh extends TMeshBackplane
{
	public float $clock = 1000.0;

	protected function now(): float
	{
		return $this->clock;
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

	/** @return array{0: TMeshBackplane, 1: SpyCluster} A secret-gated mesh node and its spy coordinator. */
	private function makeSecretNode(string $id, string $secret): array
	{
		$mesh = new TMeshBackplane();
		$mesh->setSecret($secret);
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

	public function testSilentNodeIsDeclaredDownAndItsPresenceReaped()
	{
		[$a] = $this->makeNode('A');

		$b = new ClockMesh();
		$b->setNodeTtl(10);
		$spyB = new SpyCluster('B');
		$b->setCluster($spyB);
		$this->link($a, $b);

		$a->putPresence('a-client', ['node' => 'A']);   // floods PRESENCE_SET, originNode 'A'
		$b->clock = 1000.0;
		$b->tick();   // B ingests the presence and records that node A is alive
		self::assertNotContains('A', $spyB->droppedNodes, 'A node just heard from is not declared down.');

		$b->clock = 1000.0 + 11.0;   // A never heartbeats again; advance B past the TTL
		$b->tick();
		self::assertContains('A', $spyB->droppedNodes, 'A node unheard past the TTL is declared down and its presence reaped.');
	}

	public function testAliveNodeIsNotDeclaredDown()
	{
		[$a] = $this->makeNode('A');
		$b = new ClockMesh();
		$b->setNodeTtl(10);
		$b->setCluster($spyB = new SpyCluster('B'));
		$this->link($a, $b);

		$a->putPresence('a-client', ['node' => 'A']);
		$b->clock = 1000.0;
		$b->tick();

		// A keeps heartbeating (a fresh envelope from A) before the TTL elapses.
		$a->publish(new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'A', 'alive'));
		$b->clock = 1000.0 + 8.0;
		$b->tick();   // refreshes _nodeSeen[A]
		$b->clock = 1000.0 + 12.0;
		$b->tick();   // 12 since the last scan, but only 4 since A was last heard
		self::assertNotContains('A', $spyB->droppedNodes, 'A node still being heard within the TTL stays alive.');
	}

	public function testConnectPeerDoesNotDuplicateAnInFlightDial()
	{
		$mesh = new TMeshBackplane();
		$mesh->setSecret('shh');
		new TWebSocketCluster('n', $mesh);
		$mesh->connectPeer('tcp://127.0.0.1:59991');
		$mesh->connectPeer('tcp://127.0.0.1:59991');   // the same peer is already being dialed
		self::assertSame(1, $mesh->getPendingCount(), 'A second dial to a peer already in flight is skipped.');
		$mesh->close();
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

	public function testChallengeAdmitsAPeerThatProvesTheSecret()
	{
		[$a] = $this->makeSecretNode('A', 'clustersecret');
		[$b, $spyB] = $this->makeSecretNode('B', 'clustersecret');
		[$rawA, $rawB] = TSocketStream::pair();
		$rawA->setBlocking(false);
		$rawB->setBlocking(false);
		$a->putPresence('a-1', ['node' => 'A']);   // a local client A shares only once it is trusted

		$a->addPeer(new TWebSocketConnection($rawA, true), $rawA);    // dialer: awaits the challenge
		$b->addPeer(new TWebSocketConnection($rawB, false), $rawB);   // acceptor: issues the challenge, withholds its state
		self::assertCount(0, $spyB->received, 'The acceptor delivers nothing from an unproven peer.');

		$a->tick();   // A reads the challenge, signs the nonce, and releases its join state
		$b->tick();   // B verifies the answer and accepts A's presence

		self::assertSame(1, $b->getPeerCount(), 'A peer that proves the secret stays linked.');
		$presence = array_filter($spyB->received, fn ($e) => $e->getType() === TWebSocketEnvelope::PRESENCE_SET && $e->getClientId() === 'a-1');
		self::assertCount(1, $presence, 'Once the challenge is answered, the peer\'s presence is accepted.');
	}

	public function testChallengeRejectsAReplayedHandshakeThatCannotAnswer()
	{
		[$b, $spyB] = $this->makeSecretNode('B', 'clustersecret');
		[$rawAtk, $rawB] = TSocketStream::pair();
		$rawAtk->setBlocking(false);
		$rawB->setBlocking(false);

		// An attacker replayed a captured upgrade to reach B but does not know the secret.
		$b->addPeer(new TWebSocketConnection($rawB, false), $rawB);
		self::assertSame(1, $b->getPeerCount(), 'The challenged peer is held pending its answer.');

		$attacker = new TWebSocketConnection($rawAtk, true);
		$attacker->send((new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'evil', '', null, 'evil-1', ['node' => 'evil']))->encode());
		$b->tick();   // B ignores an unverified peer's traffic
		$injected = array_filter($spyB->received, fn ($e) => $e->getClientId() === 'evil-1');
		self::assertCount(0, $injected, 'An unproven peer cannot inject presence before answering the challenge.');
		self::assertSame(1, $b->getPeerCount());

		$attacker->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_RESPONSE, 'evil', 'forged-token'))->encode());
		$b->tick();   // a wrong answer drops the peer
		self::assertSame(0, $b->getPeerCount(), 'A peer that fails the challenge is dropped.');
	}

	public function testUnansweredChallengeIsDroppedAfterTheTimeout()
	{
		$b = new ClockMesh();
		$b->setSecret('clustersecret');
		$b->setCluster(new SpyCluster('B'));
		[$rawAtk, $rawB] = TSocketStream::pair();
		$rawAtk->setBlocking(false);
		$rawB->setBlocking(false);

		$b->clock = 1000.0;
		$b->addPeer(new TWebSocketConnection($rawB, false), $rawB);   // authDeadline = 1000 + Timeout
		self::assertSame(1, $b->getPeerCount());

		$b->clock = 1000.0 + $b->getTimeout() + 1.0;   // the peer never answers; advance past the deadline
		$b->tick();
		self::assertSame(0, $b->getPeerCount(), 'A peer that never answers the challenge is dropped after the timeout.');
	}
}
