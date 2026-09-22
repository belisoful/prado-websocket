<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TConfigurationException;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\IWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\TComponent;
use Prado\Util\Clock\TMockClock;

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

	public function getLocalPresence(): array
	{
		return [];
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

class TMeshBackplaneTest extends TestCase
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

		$clock = new TMockClock();
		$b = (new TMeshBackplane())->setClock($clock);
		$b->setNodeTtl(10);
		$spyB = new SpyCluster('B');
		$b->setCluster($spyB);
		$this->link($a, $b);

		$a->putPresence('a-client', ['node' => 'A']);   // floods PRESENCE_SET, originNode 'A'
		$clock->setMicrotime(1000.0);
		$b->tick();   // B ingests the presence and records that node A is alive
		self::assertNotContains('A', $spyB->droppedNodes, 'A node just heard from is not declared down.');

		$clock->setMicrotime(1000.0 + 11.0);   // A never heartbeats again; advance B past the TTL
		$b->tick();
		self::assertContains('A', $spyB->droppedNodes, 'A node unheard past the TTL is declared down and its presence reaped.');
	}

	public function testAliveNodeIsNotDeclaredDown()
	{
		[$a] = $this->makeNode('A');
		$clock = new TMockClock();
		$b = (new TMeshBackplane())->setClock($clock);
		$b->setNodeTtl(10);
		$b->setCluster($spyB = new SpyCluster('B'));
		$this->link($a, $b);

		$a->putPresence('a-client', ['node' => 'A']);
		$clock->setMicrotime(1000.0);
		$b->tick();

		// A keeps heartbeating (a fresh envelope from A) before the TTL elapses.
		$a->publish(new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'A', 'alive'));
		$clock->setMicrotime(1000.0 + 8.0);
		$b->tick();   // refreshes _nodeSeen[A]
		$clock->setMicrotime(1000.0 + 12.0);
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
		$proof = base64_encode(hash_hmac('sha256', 'handshake:' . $key, sha1('s3cr3t'), true));   // domain-tagged for the handshake purpose
		self::assertTrue($mesh->authenticate(['sec-websocket-key' => $key, 'x-cluster-auth' => $proof]), 'A valid HMAC proof is accepted.');
		self::assertFalse($mesh->authenticate(['sec-websocket-key' => $key, 'x-cluster-auth' => 'forged']), 'A wrong proof is rejected.');
		self::assertFalse($mesh->authenticate(['sec-websocket-key' => $key]), 'A missing proof is rejected.');
		self::assertFalse($mesh->authenticate([]), 'A missing handshake key is rejected.');
	}

	public function testChallengeAnswerCannotBeForgedIntoAHandshakeProof()
	{
		// Regression for the signing-oracle: a malicious node that a legit peer dials issues a challenge
		// for an attacker-chosen value; the dialer answers by signing it.  Because the handshake proof
		// and the challenge answer are domain-separated, that harvested answer must NOT authenticate as a
		// handshake proof for the same value — otherwise the oracle would forge a way past the secret.
		[$victim] = $this->makeSecretNode('victim', 'clustersecret');
		[$rawVictim, $rawAttacker] = TSocketStream::pair();
		$rawVictim->setBlocking(false);
		$rawAttacker->setBlocking(false);

		// The victim dials the attacker (isClient = true), so it will answer the attacker's challenge.
		$victim->addPeer(new TWebSocketConnection($rawVictim, true), $rawVictim);

		// The attacker challenges with a nonce it will later try to reuse as its own Sec-WebSocket-Key.
		$forgeKey = 'attacker-chosen-key-value';
		$attacker = new TWebSocketConnection($rawAttacker, false);
		$attacker->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_CHALLENGE, 'attacker', $forgeKey))->encode());
		$victim->tick();   // the victim answers: AUTH_RESPONSE( token over $forgeKey )

		// Harvest the victim's answer (what a malicious acceptor would capture).
		$harvested = null;
		foreach ($attacker->feed($rawAttacker->read(65536)) as $message) {
			$envelope = TWebSocketEnvelope::decode($message);
			if ($envelope !== null && $envelope->getType() === TWebSocketEnvelope::AUTH_RESPONSE) {
				$harvested = $envelope->getPayload();
			}
		}
		self::assertNotNull($harvested, 'The dialer answered the challenge (oracle output captured).');

		// Replay the harvested answer as a handshake proof for key = $forgeKey against a fresh node.
		[$target] = $this->makeSecretNode('target', 'clustersecret');
		self::assertFalse(
			$target->authenticate(['sec-websocket-key' => $forgeKey, 'x-cluster-auth' => $harvested]),
			'A challenge answer harvested from a dialer must not pass as a handshake proof (domain separation).'
		);
	}

	public function testConcurrentUnverifiedPeersAreCapped()
	{
		$node = new TMeshBackplane();
		$node->setSecret('clustersecret');
		$node->setMaxPendingPeers(2);
		$node->setCluster(new SpyCluster('node'));

		$keep = [];   // hold socket refs so the pairs stay open
		for ($i = 0; $i < 5; $i++) {
			[$rawPeer, $rawRemote] = TSocketStream::pair();
			$rawPeer->setBlocking(false);
			$rawRemote->setBlocking(false);
			$keep[] = [$rawPeer, $rawRemote];
			// Inbound (acceptor) peers that never answer their challenge — a replayed-proof flood.
			$node->addPeer(new TWebSocketConnection($rawPeer, false), $rawPeer);
		}

		self::assertSame(2, $node->getPeerCount(), 'Concurrent unverified inbound peers are capped, so a flood cannot hold unbounded links.');
	}

	public function testChallengeAdmitsAPeerThatProvesTheSecret()
	{
		[$a] = $this->makeSecretNode('A', 'clustersecret');
		[$b, $spyB] = $this->makeSecretNode('B', 'clustersecret');
		[$rawA, $rawB] = TSocketStream::pair();
		$rawA->setBlocking(false);
		$rawB->setBlocking(false);
		$a->putPresence('a-1', ['node' => 'A']);   // a local client A shares only once it is trusted

		$a->addPeer(new TWebSocketConnection($rawA, true), $rawA);    // each side challenges the other
		$b->addPeer(new TWebSocketConnection($rawB, false), $rawB);
		self::assertCount(0, $spyB->received, 'Neither side delivers anything from an unproven peer.');

		// Mutual auth takes two round-trips: each side answers the other's challenge, then verifies the
		// other's answer before releasing its join state.
		for ($round = 0; $round < 3; $round++) {
			$a->tick();
			$b->tick();
		}

		self::assertSame(1, $b->getPeerCount(), 'A peer that proves the secret stays linked.');
		$presence = array_filter($spyB->received, fn ($e) => $e->getType() === TWebSocketEnvelope::PRESENCE_SET && $e->getClientId() === 'a-1');
		self::assertCount(1, $presence, 'Once both sides verify each other, the peer\'s presence is accepted.');
	}

	public function testDialedPeerGetsNoStateUntilItProvesTheSecret()
	{
		// Mutual-auth property: a node this one dials (e.g. a poisoned gossip URI pointing at an attacker)
		// must not learn this node's presence or advertised URI unless it answers this node's challenge.
		[$a] = $this->makeSecretNode('A', 'clustersecret');
		$a->setAdvertise('tcp://node-a:9');
		$a->putPresence('a-private-client', ['node' => 'A', 'user' => 'alice']);

		[$rawA, $rawEvil] = TSocketStream::pair();
		$rawA->setBlocking(false);
		$rawEvil->setBlocking(false);
		$a->addPeer(new TWebSocketConnection($rawA, true), $rawA);   // A dials the (secret-less) endpoint

		// The endpoint (server side of A's dial) reads whatever A sends but never answers A's challenge.
		$evil = new TWebSocketConnection($rawEvil, false);
		$leaked = [];
		for ($round = 0; $round < 3; $round++) {
			$a->tick();
			foreach ($evil->feed($rawEvil->read(65536)) as $message) {
				$envelope = TWebSocketEnvelope::decode($message);
				if ($envelope !== null) {
					$leaked[] = $envelope->getType();
				}
			}
		}

		self::assertContains(TWebSocketEnvelope::AUTH_CHALLENGE, $leaked, 'A challenges the peer it dialed.');
		self::assertNotContains(TWebSocketEnvelope::PRESENCE_SET, $leaked, 'An unproven dialed peer never receives this node\'s presence.');
		self::assertNotContains(TWebSocketEnvelope::NODE_UP, $leaked, 'An unproven dialed peer never receives this node\'s advertised URI.');
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
		$clock = new TMockClock();
		$b = (new TMeshBackplane())->setClock($clock);
		$b->setSecret('clustersecret');
		$b->setCluster(new SpyCluster('B'));
		[$rawAtk, $rawB] = TSocketStream::pair();
		$rawAtk->setBlocking(false);
		$rawB->setBlocking(false);

		$clock->setMicrotime(1000.0);
		$b->addPeer(new TWebSocketConnection($rawB, false), $rawB);   // authDeadline = 1000 + Timeout
		self::assertSame(1, $b->getPeerCount());

		$clock->setMicrotime(1000.0 + $b->getTimeout() + 1.0);   // the peer never answers; advance past the deadline
		$b->tick();
		self::assertSame(0, $b->getPeerCount(), 'A peer that never answers the challenge is dropped after the timeout.');
	}

	/**
	 * Accepts an attacker link on a secret-gated node and captures the challenge nonce it is issued.
	 * @return array{0: TMeshBackplane, 1: SpyCluster, 2: TWebSocketConnection, 3: TSocketStream, 4: string} The node, its spy, the attacker link, the attacker transport, and the nonce.
	 */
	private function challengedAttacker(): array
	{
		[$b, $spyB] = $this->makeSecretNode('B', 'clustersecret');
		[$rawAtk, $rawB] = TSocketStream::pair();
		$rawAtk->setBlocking(false);
		$rawB->setBlocking(false);
		$b->addPeer(new TWebSocketConnection($rawB, false), $rawB);
		$attacker = new TWebSocketConnection($rawAtk, true);
		$nonce = '';
		foreach ($attacker->feed($rawAtk->read(65536)) as $message) {
			$envelope = TWebSocketEnvelope::decode($message);
			if ($envelope !== null && $envelope->getType() === TWebSocketEnvelope::AUTH_CHALLENGE) {
				$nonce = $envelope->getPayload();
			}
		}
		self::assertNotSame('', $nonce, 'The accepted peer is challenged.');
		return [$b, $spyB, $attacker, $rawAtk, $nonce];
	}

	/** @return string The answer the implementation expects for a nonce from a given node. */
	private function answerFor(string $nonce, string $answerer, string $secret = 'clustersecret'): string
	{
		return base64_encode(hash_hmac('sha256', 'challenge:' . $nonce . "\0" . $answerer, sha1($secret), true));
	}

	public function testChallengeCannotBeReflectedBackAtItsIssuer()
	{
		// An attacker without the secret echoes B's own nonce as a challenge, hoping B signs it and
		// hands back the very answer B is waiting for.
		[$b, , $attacker, $rawAtk, $nonce] = $this->challengedAttacker();
		$attacker->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_CHALLENGE, 'attacker', $nonce))->encode());
		$b->tick();

		self::assertSame(0, $b->getPeerCount(), 'A peer that reflects the challenge is dropped.');
		$answers = [];
		try {
			foreach ($attacker->feed($rawAtk->read(65536)) as $message) {
				$envelope = TWebSocketEnvelope::decode($message);
				if ($envelope !== null && $envelope->getType() === TWebSocketEnvelope::AUTH_RESPONSE) {
					$answers[] = $envelope->getPayload();
				}
			}
		} catch (\Throwable $e) {
			// The link was closed under the attacker; only the frames before the close matter.
		}
		self::assertSame([], $answers, 'B never signs a nonce it issued itself.');
	}

	public function testAnswerClaimingTheIssuersOwnIdIsRefused()
	{
		// Even a correctly computed answer is refused when it claims to come from B itself, so an
		// answer B produced can never verify a peer to B.
		[$b, , $attacker, , $nonce] = $this->challengedAttacker();
		$attacker->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_RESPONSE, 'B', $this->answerFor($nonce, 'B')))->encode());
		$b->tick();
		self::assertSame(0, $b->getPeerCount(), 'An answer bearing the verifier\'s own node id fails the challenge.');
	}

	public function testAnswerIsBoundToTheAnsweringNode()
	{
		[$b, $spyB, $attacker, , $nonce] = $this->challengedAttacker();
		// The right token for the wrong claimed node fails.
		$attacker->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_RESPONSE, 'attacker', $this->answerFor($nonce, 'someone-else')))->encode());
		$b->tick();
		self::assertSame(0, $b->getPeerCount(), 'An answer computed for another node id fails the challenge.');

		// Positive control: a holder of the secret answering under its own id is verified.
		[$b, $spyB, $peer, , $nonce] = $this->challengedAttacker();
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_RESPONSE, 'peer', $this->answerFor($nonce, 'peer')))->encode());
		$b->tick();
		self::assertSame(1, $b->getPeerCount(), 'A correct answer under the answering node\'s id verifies the peer.');
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'peer', '', null, 'peer-1', ['node' => 'peer']))->encode());
		$b->tick();
		$accepted = array_filter($spyB->received, fn ($e) => $e->getClientId() === 'peer-1');
		self::assertCount(1, $accepted, 'The verified peer\'s traffic flows.');
	}

	/**
	 * Accepts a link on a secret-gated node B and verifies it as node `$id` by answering B's challenge.
	 * @param string $id
	 * @return array{0: TMeshBackplane, 1: SpyCluster, 2: TWebSocketConnection, 3: TSocketStream} B, its spy, the peer link, and the peer transport.
	 */
	private function verifiedPeer(string $id): array
	{
		[$b, $spyB, $peer, $raw, $nonce] = $this->challengedAttacker();
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_RESPONSE, $id, $this->answerFor($nonce, $id)))->encode());
		$b->tick();
		self::assertSame(1, $b->getPeerCount());
		return [$b, $spyB, $peer, $raw];
	}

	public function testOpenRequiresASecret()
	{
		[$mesh] = $this->makeNode('A');
		$this->expectException(TConfigurationException::class);
		$mesh->open();
	}

	public function testADeadNodesLinkIsDroppedSoItsUriCanBeRedialed()
	{
		$clock = new TMockClock();
		$clock->setMicrotime(1000.0);
		[$a] = $this->makeSecretNode('A', 'clustersecret');
		[$b] = $this->makeSecretNode('B', 'clustersecret');
		$a->setClock($clock);
		$b->setClock($clock);
		[$rawA, $rawB] = TSocketStream::pair();
		$rawA->setBlocking(false);
		$rawB->setBlocking(false);
		$a->addPeer(new TWebSocketConnection($rawA, false), $rawA);
		$b->addPeer(new TWebSocketConnection($rawB, true), $rawB, 'tcp://127.0.0.1:1');   // B dialed A at this URI
		for ($round = 0; $round < 4; $round++) {
			$a->tick();   // mutual verification, then A's first heartbeat proves it alive to B
			$b->tick();
		}
		self::assertSame(1, $b->getPeerCount());
		$b->connectPeer('tcp://127.0.0.1:1');
		self::assertSame(0, $b->getPendingCount(), 'A linked URI is not dialed again.');

		// A falls silent (a half-open link keeps reading nothing) past the TTL.
		$clock->setMicrotime(1000.0 + $b->getNodeTtl() + 1);
		$b->tick();
		self::assertSame(0, $b->getPeerCount(), 'The link to a node declared down is dropped.');
		$b->connectPeer('tcp://127.0.0.1:1');
		self::assertSame(1, $b->getPendingCount(), 'The dead node\'s URI is free to be dialed again.');
		$b->close();
		$a->close();
	}

	public function testUnlinkedSeedPeersAreRedialedOncePerTtl()
	{
		$clock = new TMockClock();
		$clock->setMicrotime(1000.0);
		[$mesh] = $this->makeSecretNode('A', 'clustersecret');
		$mesh->setClock($clock);
		$mesh->setPeers(['tcp://127.0.0.1:1']);   // nothing listens there
		$mesh->setTimeout(2);
		$mesh->open();
		self::assertSame(1, $mesh->getPendingCount(), 'The seed is dialed on open.');

		$clock->setMicrotime(1003.0);   // past the dial timeout: the attempt fails and the first re-dial is due
		$mesh->tick();
		self::assertSame(1, $mesh->getPendingCount(), 'A failed seed dial is retried.');
		$clock->setMicrotime(1006.0);
		$mesh->tick();
		self::assertSame(0, $mesh->getPendingCount(), 'Re-dials are throttled to once per TTL.');
		$clock->setMicrotime(1006.0 + $mesh->getNodeTtl());
		$mesh->tick();
		self::assertSame(1, $mesh->getPendingCount(), 'The seed is dialed again after a TTL.');
		$mesh->close();
	}

	public function testARelayedAnnounceDoesNotBindADistantUriToTheDirectPeer()
	{
		[$b, , $peer] = $this->verifiedPeer('A');
		// A relays C's announce first: B must dial C, not bind C's URI to A's link.
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'C', '', null, null, ['uri' => 'tcp://127.0.0.1:2']))->encode());
		$b->tick();
		self::assertSame(1, $b->getPendingCount(), 'A distant node announced through the peer is dialed.');
		// Then A announces itself: that binds to the link, so B never dials A's URI.
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'A', '', null, null, ['uri' => 'tcp://127.0.0.1:3']))->encode());
		$b->tick();
		$pending = $b->getPendingCount();   // the refused dial to C may already have failed; only the delta matters
		$b->connectPeer('tcp://127.0.0.1:3');
		self::assertSame($pending, $b->getPendingCount(), 'The direct peer\'s own URI is bound to its link and not dialed.');
		$b->close();
	}

	public function testTrafficUnderThisNodesOwnIdFromAPeerIsIgnored()
	{
		[$b, $spyB, $peer] = $this->verifiedPeer('A');
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'B', '', null, 'B-x', ['node' => 'B']))->encode());
		$b->tick();
		self::assertCount(0, array_filter($spyB->received, fn ($e) => $e->getClientId() === 'B-x'), 'An envelope claiming this node\'s own id (a NodeId clash) is not routed.');
		self::assertSame(1, $b->getPeerCount());
		$b->close();
	}
}
