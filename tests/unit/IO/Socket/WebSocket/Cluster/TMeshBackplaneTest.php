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
use Prado\IO\Stream\TBufferStream;
use Prado\TComponent;
use Prado\Test\Unit\IO\Socket\WebSocket\TTestMonotonicClock;

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

	/** @return TWebSocketEnvelope[] The received envelopes other than node announces (a new link's join state starts with one). */
	public function traffic(): array
	{
		return array_values(array_filter($this->received, fn ($e) => $e->getType() !== TWebSocketEnvelope::NODE_UP));
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

	/**
	 * Links two mesh nodes with a connected socket pair and returns the transports.
	 * @param TMeshBackplane $a
	 * @param TMeshBackplane $b
	 * @return array{0: TSocketStream, 1: TSocketStream} The dialer's and the acceptor's transports.
	 */
	private function linkPair(TMeshBackplane $a, TMeshBackplane $b): array
	{
		[$rawA, $rawB] = TSocketStream::pair();
		$rawA->setBlocking(false);
		$rawB->setBlocking(false);
		$a->addPeer(new TWebSocketConnection($rawA, true), $rawA);
		$b->addPeer(new TWebSocketConnection($rawB, false), $rawB);
		return [$rawA, $rawB];
	}

	/** @return string A `tcp://` URI on a loopback port the OS chose and nothing listens on. */
	private function closedPortUri(): string
	{
		$listener = TSocketStream::bind('tcp://127.0.0.1:0', STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);
		$uri = 'tcp://127.0.0.1:' . $listener->getLocalAddress()->getPort();
		$listener->close();
		return $uri;
	}

	/** @return array{0: TWebSocketCluster, 1: TMeshBackplane} A coordinator on a mesh backplane. */
	private function makeClusterNode(string $id, TTestMonotonicClock $clock): array
	{
		$mesh = new TMeshBackplane();
		$mesh->setMonotonicClock($clock);
		return [new TWebSocketCluster($id, $mesh), $mesh];
	}

	public function testTickDrainsALinkThatTheKernelDidNotTakeAtOnce()
	{
		[$a] = $this->makeNode('A');
		[$b, $spyB] = $this->makeNode('B');
		[$rawA] = $this->linkPair($a, $b);
		$b->tick();

		$a->publish(new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'A', str_repeat('x', 1024 * 1024)));
		self::assertSame([$rawA], $a->getWriteSources(), 'The link holds output the socket buffer could not take.');
		for ($i = 0; $i < 2000 && $a->getWriteSources() !== []; $i++) {
			$a->tick();   // no further traffic: only the tick flushes the link
			$b->tick();
		}
		$b->tick();

		self::assertSame([], $a->getWriteSources(), 'The queued output drains.');
		self::assertCount(1, $spyB->traffic(), 'The whole message reaches the peer.');
		self::assertSame(1024 * 1024, strlen($spyB->traffic()[0]->getPayload()));
	}

	public function testFlushSourceDrainsTheWritableLink()
	{
		[$a] = $this->makeNode('A');
		[$b, $spyB] = $this->makeNode('B');
		[$rawA] = $this->linkPair($a, $b);
		$b->tick();

		$a->publish(new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'A', str_repeat('y', 1024 * 1024)));
		[$stranger] = TSocketStream::pair();
		$a->flushSource($stranger);   // a source the backplane does not hold is ignored
		self::assertSame([$rawA], $a->getWriteSources());
		for ($i = 0; $i < 2000 && $a->getWriteSources() !== []; $i++) {
			$a->flushSource($rawA);   // the server reports the link writable
			$b->tick();
		}
		$b->tick();

		self::assertSame([], $a->getWriteSources(), 'Flushing the writable link drains it.');
		self::assertCount(1, $spyB->traffic(), 'The whole message reaches the peer.');
	}

	public function testAFailedFlushDropsThePeer()
	{
		[$a] = $this->makeNode('A');
		[$b] = $this->makeNode('B');
		[$rawA, $rawB] = $this->linkPair($a, $b);
		$a->publish(new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'A', str_repeat('z', 1024 * 1024)));
		self::assertSame([$rawA], $a->getWriteSources());

		$rawB->close();   // the peer goes away with output still queued
		for ($i = 0; $i < 10 && $a->getPeerCount() > 0; $i++) {
			$a->flushSource($rawA);
		}

		self::assertSame(0, $a->getPeerCount(), 'A link whose write fails is dropped.');
		self::assertSame([], $a->getWriteSources());
	}

	public function testARestartedNodesPreviousClientsAreDroppedOnItsNewIncarnation()
	{
		$clock = new TTestMonotonicClock();
		$clock->setSeconds(1000.0);
		[$ca, $a] = $this->makeClusterNode('A', $clock);
		[$cb1, $b1] = $this->makeClusterNode('B', $clock);
		[, $rawB1] = $this->linkPair($a, $b1);
		$oldId = $cb1->register(new TWebSocketConnection(new TBufferStream(), false));
		$a->tick();
		self::assertArrayHasKey($oldId, $ca->presence());

		$rawB1->close();   // B exits
		$a->tick();
		[$cb2, $b2] = $this->makeClusterNode('B', $clock);   // and restarts at once under the same node id
		$newId = $cb2->register(new TWebSocketConnection(new TBufferStream(), false));
		$this->linkPair($a, $b2);
		$a->tick();

		self::assertArrayNotHasKey($oldId, $ca->presence(), "The previous incarnation's client is dropped.");
		self::assertArrayHasKey($newId, $ca->presence(), "The new incarnation's client is present.");
	}

	public function testARestartIsDetectedBehindAnIntermediary()
	{
		$clock = new TTestMonotonicClock();
		$clock->setSeconds(1000.0);
		[$cp, $p] = $this->makeClusterNode('P', $clock);
		[, $q] = $this->makeClusterNode('Q', $clock);
		[$cr1, $r1] = $this->makeClusterNode('R', $clock);
		$this->linkPair($p, $q);
		[, $rawR1] = $this->linkPair($q, $r1);   // a line: P - Q - R
		$oldId = $cr1->register(new TWebSocketConnection(new TBufferStream(), false));
		$q->tick();
		$p->tick();
		self::assertArrayHasKey($oldId, $cp->presence());

		$rawR1->close();
		$q->tick();
		[$cr2, $r2] = $this->makeClusterNode('R', $clock);
		$newId = $cr2->register(new TWebSocketConnection(new TBufferStream(), false));
		$this->linkPair($q, $r2);
		$q->tick();   // relays R's new announce and presence
		$p->tick();

		self::assertArrayNotHasKey($oldId, $cp->presence(), "A node that is not R's direct peer drops R's previous incarnation too.");
		self::assertArrayHasKey($newId, $cp->presence());
	}

	public function testAnUnchangedOrMissingIncarnationKeepsTheNodesClients()
	{
		[$b, $spyB, $peer] = $this->verifiedPeer('A');
		$announce = fn (array $meta) => $peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'A', '', null, null, $meta))->encode());
		$announce(['uri' => '']);   // a node predating incarnations
		$announce(['uri' => '', 'incarnation' => 'one']);
		$announce(['uri' => '', 'incarnation' => 'one']);   // a heartbeat from the same incarnation
		$announce(['uri' => '']);
		$b->tick();
		self::assertSame([], $spyB->droppedNodes, 'Only a changed incarnation drops the node\'s clients.');

		$announce(['uri' => '', 'incarnation' => 'two']);
		$b->tick();
		self::assertSame(['A'], $spyB->droppedNodes, 'A new incarnation drops the clients held for the node.');
		$b->close();
	}

	public function testJoinStateAnnouncesTheNodeBeforeItsPresence()
	{
		[$a] = $this->makeNode('A');
		[$b, $spyB] = $this->makeNode('B');
		$a->putPresence('A-1', ['node' => 'A']);
		$this->link($a, $b);
		$b->tick();

		$types = array_map(fn ($e) => $e->getType(), $spyB->received);
		self::assertSame([TWebSocketEnvelope::NODE_UP, TWebSocketEnvelope::PRESENCE_SET], $types, 'The announce precedes the presence, even when the node advertises no URI.');
		self::assertNotSame('', $spyB->received[0]->getMeta()['incarnation'] ?? '', 'The announce carries the incarnation.');
	}

	public function testPublishFloodsToTheLinkedPeer()
	{
		[$a, $spyA] = $this->makeNode('A');
		[$b, $spyB] = $this->makeNode('B');
		$this->link($a, $b);

		$a->publish(new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'A', 'hello', 'news'));
		$b->tick();

		self::assertCount(1, $spyB->traffic(), 'The peer delivers the flooded envelope.');
		self::assertSame('hello', $spyB->traffic()[0]->getPayload());
		self::assertSame('news', $spyB->traffic()[0]->getChannel());
		self::assertCount(0, $spyA->received, 'A node does not deliver its own published envelope to itself.');
	}

	public function testBroadcastReachesThePeer()
	{
		[$a] = $this->makeNode('A');
		[$b, $spyB] = $this->makeNode('B');
		$this->link($a, $b);

		$a->publish(new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'A', 'all'));
		$b->tick();

		self::assertCount(1, $spyB->traffic());
		self::assertSame(TWebSocketEnvelope::BROADCAST, $spyB->traffic()[0]->getType());
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
		self::assertCount(1, $spyB->traffic());
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

		$clock = new TTestMonotonicClock();
		$b = (new TMeshBackplane())->setMonotonicClock($clock);
		$b->setNodeTtl(10);
		$spyB = new SpyCluster('B');
		$b->setCluster($spyB);
		$this->link($a, $b);

		$a->putPresence('a-client', ['node' => 'A']);   // floods PRESENCE_SET, originNode 'A'
		$clock->setSeconds(1000.0);
		$b->tick();   // B ingests the presence and records that node A is alive
		self::assertNotContains('A', $spyB->droppedNodes, 'A node just heard from is not declared down.');

		$clock->setSeconds(1000.0 + 11.0);   // A never heartbeats again; advance B past the TTL
		$b->tick();
		self::assertContains('A', $spyB->droppedNodes, 'A node unheard past the TTL is declared down and its presence reaped.');
	}

	public function testAliveNodeIsNotDeclaredDown()
	{
		[$a] = $this->makeNode('A');
		$clock = new TTestMonotonicClock();
		$b = (new TMeshBackplane())->setMonotonicClock($clock);
		$b->setNodeTtl(10);
		$b->setCluster($spyB = new SpyCluster('B'));
		$this->link($a, $b);

		$a->putPresence('a-client', ['node' => 'A']);
		$clock->setSeconds(1000.0);
		$b->tick();

		// A keeps heartbeating (a fresh envelope from A) before the TTL elapses.
		$a->publish(new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'A', 'alive'));
		$clock->setSeconds(1000.0 + 8.0);
		$b->tick();   // refreshes _nodeSeen[A]
		$clock->setSeconds(1000.0 + 12.0);
		$b->tick();   // 12 since the last scan, but only 4 since A was last heard
		self::assertNotContains('A', $spyB->droppedNodes, 'A node still being heard within the TTL stays alive.');
	}

	public function testConnectPeerDoesNotDuplicateAnInFlightDial()
	{
		$mesh = new TMeshBackplane();
		$mesh->setSecret('shh');
		new TWebSocketCluster('n', $mesh);
		$listener = TSocketStream::bind('tcp://127.0.0.1:0', STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);   // an OS-chosen port; never accepted, so the dial stays in flight
		$peer = 'tcp://127.0.0.1:' . $listener->getLocalAddress()->getPort();
		$mesh->connectPeer($peer);
		$mesh->connectPeer($peer);   // the same peer is already being dialed
		self::assertSame(1, $mesh->getPendingCount(), 'A second dial to a peer already in flight is skipped.');
		$mesh->close();
		$listener->close();
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

	public function testAnAcceptedPeerCannotUseOneNodeToAnswerAnothersChallenge()
	{
		// An attacker without the secret replays a captured handshake to A and to B, then asks each
		// node to sign the other's nonce. Neither answers an accepted peer before verifying it.
		[$a, $spyA] = $this->makeSecretNode('A', 'clustersecret');
		[$b, $spyB] = $this->makeSecretNode('B', 'clustersecret');
		$b->putPresence('b-1', ['user' => 'alice']);
		$attach = function (TMeshBackplane $node): array {
			[$rawAtk, $rawNode] = TSocketStream::pair();
			$rawAtk->setBlocking(false);
			$rawNode->setBlocking(false);
			$node->addPeer(new TWebSocketConnection($rawNode, false), $rawNode);
			return [new TWebSocketConnection($rawAtk, true), $rawAtk];
		};
		$read = function (TWebSocketConnection $link, TSocketStream $raw): array {
			$envelopes = [];
			$bytes = $raw->read(65536);
			foreach ($bytes === '' ? [] : $link->feed($bytes) as $message) {
				if (($envelope = TWebSocketEnvelope::decode($message)) !== null) {
					$envelopes[$envelope->getType()][] = $envelope;
				}
			}
			return $envelopes;
		};
		[$linkA, $rawA] = $attach($a);
		[$linkB, $rawB] = $attach($b);
		$nonceA = $read($linkA, $rawA)[TWebSocketEnvelope::AUTH_CHALLENGE][0]->getPayload();
		$nonceB = $read($linkB, $rawB)[TWebSocketEnvelope::AUTH_CHALLENGE][0]->getPayload();

		$linkA->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_CHALLENGE, 'x', $nonceB))->encode());
		$linkB->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_CHALLENGE, 'x', $nonceA))->encode());
		$a->tick();
		$b->tick();
		self::assertArrayNotHasKey(TWebSocketEnvelope::AUTH_RESPONSE, $read($linkA, $rawA), 'A does not sign a nonce for a peer it has not verified.');
		self::assertArrayNotHasKey(TWebSocketEnvelope::AUTH_RESPONSE, $read($linkB, $rawB), 'B does not sign a nonce for a peer it has not verified.');

		$linkB->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_RESPONSE, 'A', 'unsigned'))->encode());
		$b->tick();
		self::assertSame(0, $b->getPeerCount(), 'Without a signature to relay, the attacker fails B\'s challenge.');
		self::assertCount(0, $spyA->received);
		self::assertCount(0, $spyB->received);
	}

	public function testAnAcceptedPeersChallengeIsAnsweredOnceItIsVerified()
	{
		[$b] = $this->makeSecretNode('B', 'clustersecret');
		[$rawDialer, $rawB] = TSocketStream::pair();
		$rawDialer->setBlocking(false);
		$rawB->setBlocking(false);
		$b->addPeer(new TWebSocketConnection($rawB, false), $rawB);
		$dialer = new TWebSocketConnection($rawDialer, true);
		$nonceB = '';
		foreach ($dialer->feed($rawDialer->read(65536)) as $message) {
			$nonceB = TWebSocketEnvelope::decode($message)->getPayload();
		}

		$dialer->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_CHALLENGE, 'D', 'dialer-nonce'))->encode());
		$b->tick();
		self::assertSame('', $rawDialer->read(65536), 'The challenge is held until the dialer is verified.');

		$dialer->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_RESPONSE, 'D', $this->answerFor($nonceB, 'D')))->encode());
		$b->tick();
		$types = [];
		foreach ($dialer->feed($rawDialer->read(65536)) as $message) {
			$envelope = TWebSocketEnvelope::decode($message);
			$types[] = $envelope->getType();
			if ($envelope->getType() === TWebSocketEnvelope::AUTH_RESPONSE) {
				self::assertSame($this->answerFor('dialer-nonce', 'B'), $envelope->getPayload());
			}
		}
		self::assertSame(TWebSocketEnvelope::AUTH_RESPONSE, $types[0] ?? null, 'Once verified, the dialer gets the answer, ahead of any join state.');
	}

	public function testUnansweredChallengeIsDroppedAfterTheTimeout()
	{
		$clock = new TTestMonotonicClock();
		$b = (new TMeshBackplane())->setMonotonicClock($clock);
		$b->setSecret('clustersecret');
		$b->setCluster(new SpyCluster('B'));
		[$rawAtk, $rawB] = TSocketStream::pair();
		$rawAtk->setBlocking(false);
		$rawB->setBlocking(false);

		$clock->setSeconds(1000.0);
		$b->addPeer(new TWebSocketConnection($rawB, false), $rawB);   // authDeadline = 1000 + Timeout
		self::assertSame(1, $b->getPeerCount());

		$clock->setSeconds(1000.0 + $b->getTimeout() + 1.0);   // the peer never answers; advance past the deadline
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

	public function testEnvelopesBeforeAMalformedFrameInTheSameReadAreReceived()
	{
		[$b, $spyB, $peer, $raw] = $this->verifiedPeer('A');
		$spyB->received = [];
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'A', 'hello', 'news'))->encode());
		$raw->write("\x83\x80\x00\x00\x00\x00");   // a masked frame with a reserved opcode, in the same read
		$b->tick();
		self::assertSame(0, $b->getPeerCount(), 'The malformed frame drops the peer.');
		self::assertSame([TWebSocketEnvelope::PUBLISH], array_map(fn ($e) => $e->getType(), $spyB->received), 'The envelope decoded before it is still received.');
		$b->close();
	}

	public function testOpenRequiresASecret()
	{
		[$mesh] = $this->makeNode('A');
		$this->expectException(TConfigurationException::class);
		$mesh->open();
	}

	public function testADeadNodesLinkIsDroppedSoItsUriCanBeRedialed()
	{
		$clock = new TTestMonotonicClock();
		$clock->setSeconds(1000.0);
		[$a] = $this->makeSecretNode('A', 'clustersecret');
		[$b] = $this->makeSecretNode('B', 'clustersecret');
		$a->setMonotonicClock($clock);
		$b->setMonotonicClock($clock);
		[$rawA, $rawB] = TSocketStream::pair();
		$rawA->setBlocking(false);
		$rawB->setBlocking(false);
		$a->addPeer(new TWebSocketConnection($rawA, false), $rawA);
		$uriA = $this->closedPortUri();
		$b->addPeer(new TWebSocketConnection($rawB, true), $rawB, $uriA);   // B dialed A at this URI
		for ($round = 0; $round < 4; $round++) {
			$a->tick();   // mutual verification, then A's first heartbeat proves it alive to B
			$b->tick();
		}
		self::assertSame(1, $b->getPeerCount());
		$b->connectPeer($uriA);
		self::assertSame(0, $b->getPendingCount(), 'A linked URI is not dialed again.');

		// A falls silent (a half-open link keeps reading nothing) past the TTL.
		$clock->setSeconds(1000.0 + $b->getNodeTtl() + 1);
		$b->tick();
		self::assertSame(0, $b->getPeerCount(), 'The link to a node declared down is dropped.');
		$b->connectPeer($uriA);
		self::assertSame(1, $b->getPendingCount(), 'The dead node\'s URI is free to be dialed again.');
		$b->close();
		$a->close();
	}

	public function testUnlinkedSeedPeersAreRedialedOncePerTtl()
	{
		$clock = new TTestMonotonicClock();
		$clock->setSeconds(1000.0);
		[$mesh] = $this->makeSecretNode('A', 'clustersecret');
		$mesh->setMonotonicClock($clock);
		$mesh->setPeers([$this->closedPortUri()]);   // nothing listens there
		$mesh->setTimeout(2);
		$mesh->open();
		self::assertSame(1, $mesh->getPendingCount(), 'The seed is dialed on open.');

		$clock->setSeconds(1003.0);   // past the dial timeout: the attempt fails and the first re-dial is due
		$mesh->tick();
		self::assertSame(1, $mesh->getPendingCount(), 'A failed seed dial is retried.');
		$clock->setSeconds(1006.0);
		$mesh->tick();
		self::assertSame(0, $mesh->getPendingCount(), 'Re-dials are throttled to once per TTL.');
		$clock->setSeconds(1006.0 + $mesh->getNodeTtl());
		$mesh->tick();
		self::assertSame(1, $mesh->getPendingCount(), 'The seed is dialed again after a TTL.');
		$mesh->close();
	}

	public function testARelayedAnnounceDoesNotBindADistantUriToTheDirectPeer()
	{
		[$b, , $peer] = $this->verifiedPeer('A');
		// A relays C's announce first: B must dial C, not bind C's URI to A's link.
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'C', '', null, null, ['uri' => $this->closedPortUri()]))->encode());
		$b->tick();
		self::assertSame(1, $b->getPendingCount(), 'A distant node announced through the peer is dialed.');
		// Then A announces itself: that binds to the link, so B never dials A's URI.
		$uriA = $this->closedPortUri();
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'A', '', null, null, ['uri' => $uriA]))->encode());
		$b->tick();
		$pending = $b->getPendingCount();   // the refused dial to C may already have failed; only the delta matters
		$b->connectPeer($uriA);
		self::assertSame($pending, $b->getPendingCount(), 'The direct peer\'s own URI is bound to its link and not dialed.');
		$b->close();
	}

	public function testATlsDialDoesNotBlockOnAPeerThatStallsTheHandshake()
	{
		if (!extension_loaded('openssl')) {
			self::markTestSkipped('ext-openssl is not available.');
		}
		$clock = new TTestMonotonicClock(1000.0);
		[$mesh] = $this->makeSecretNode('A', 'clustersecret');
		$mesh->setMonotonicClock($clock);
		$mesh->setTimeout(0.5);
		$listener = TSocketStream::bind('tcp://127.0.0.1:0', STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);   // accepts TCP in its backlog and never speaks TLS
		$uri = 'tls://127.0.0.1:' . $listener->getLocalAddress()->getPort();

		$mesh->connectPeer($uri);   // returns at once: no TLS handshake inside the connect
		self::assertSame(1, $mesh->getPendingCount(), 'The dial is in flight, not failed inside a blocking handshake.');
		for ($i = 0; $i < 5; $i++) {
			$mesh->tick();   // the connect completes and the handshake waits for a reply that never comes
		}
		self::assertSame(1, $mesh->getPendingCount(), 'The stalled handshake waits without blocking.');

		$clock->advance(1.0);   // past the dial timeout
		$mesh->tick();
		self::assertSame(0, $mesh->getPendingCount(), 'A handshake that does not finish by the deadline fails the dial.');
		$mesh->close();
		$listener->close();
	}

	public function testAConnectingDialIsAWriteSourceThatWritabilityAdvances()
	{
		[$mesh] = $this->makeSecretNode('A', 'clustersecret');
		$listener = TSocketStream::bind('tcp://127.0.0.1:0', STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);
		$mesh->connectPeer('tcp://127.0.0.1:' . $listener->getLocalAddress()->getPort());
		$sources = $mesh->getWriteSources();
		self::assertCount(1, $sources, 'A connecting dial is watched for writability, which reports its connect.');
		self::assertSame($mesh->getSources()[0], $sources[0]);

		for ($i = 0; $i < 200 && $mesh->getWriteSources() !== []; $i++) {
			$mesh->flushSource($sources[0]);   // the server reports the dial writable; no cluster tick runs
			usleep(1000);
		}

		self::assertSame([], $mesh->getWriteSources(), 'Writability completes the connect and sends the upgrade, without a tick.');
		self::assertSame(1, $mesh->getPendingCount(), 'The dial now waits for the upgrade response.');
		$accepted = stream_socket_accept($listener->getResource(), 1.0);
		self::assertIsResource($accepted);
		stream_set_blocking($accepted, false);
		$request = '';
		for ($i = 0; $i < 200 && !str_contains($request, "\r\n\r\n"); $i++) {
			$request .= (string) fread($accepted, 4096);
			usleep(1000);
		}
		self::assertStringStartsWith('GET /cluster HTTP/1.1', $request, 'The upgrade request was sent from flushSource().');
		fclose($accepted);
		$mesh->close();
		$listener->close();
	}

	public function testAnUnverifiedAcceptedPeerCannotSendALargeMessage()
	{
		[$b, , $attacker] = $this->challengedAttacker();
		$attacker->send((new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'evil', '', null, 'evil-1', ['pad' => str_repeat('x', 2 * TMeshBackplane::UNVERIFIED_MESSAGE_SIZE)]))->encode());
		$b->tick();
		self::assertSame(0, $b->getPeerCount(), 'A message over the unverified limit drops the peer before it is buffered.');
	}

	public function testAVerifiedPeerGetsTheLinksFullMessageSizeBack()
	{
		[$b, $spyB, $peer] = $this->verifiedPeer('A');
		$payload = str_repeat('y', 64 * 1024);
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'A', $payload))->encode());
		for ($i = 0; $i < 200 && $spyB->traffic() === []; $i++) {
			$peer->flushOutbound();
			$b->tick();
		}
		self::assertSame(1, $b->getPeerCount(), 'A verified peer may send a large message.');
		self::assertSame($payload, $spyB->traffic()[0]->getPayload() ?? null);
	}

	/**
	 * Pumps mesh nodes, moving the clock forward between rounds.
	 * @param TTestMonotonicClock $clock
	 * @param TMeshBackplane[] $nodes
	 * @param int $rounds
	 * @param float $step
	 */
	private function pump(TTestMonotonicClock $clock, array $nodes, int $rounds, float $step = 0.5): void
	{
		for ($i = 0; $i < $rounds; $i++) {
			foreach ($nodes as $node) {
				$node->tick();
			}
			$clock->advance($step);
		}
	}

	/** @return array{0: TWebSocketCluster, 1: TMeshBackplane} A coordinator on a mesh backplane with a short TTL. */
	private function makeLineNode(string $id, TTestMonotonicClock $clock): array
	{
		[$cluster, $mesh] = $this->makeClusterNode($id, $clock);
		$mesh->setNodeTtl(3);
		return [$cluster, $mesh];
	}

	public function testPresenceBehindAnIntermediaryConvergesAfterATransientSilence()
	{
		$clock = new TTestMonotonicClock(1000.0);
		[$ca, $a] = $this->makeLineNode('A', $clock);
		[, $b] = $this->makeLineNode('B', $clock);
		[$cc, $c] = $this->makeLineNode('C', $clock);
		$this->linkPair($a, $b);
		[$rawB] = $this->linkPair($b, $c);   // a line: A - B - C
		$a1 = $ca->register(new TWebSocketConnection(new TBufferStream(), false));
		$c1 = $cc->register(new TWebSocketConnection(new TBufferStream(), false));
		$this->pump($clock, [$a, $b, $c], 6);
		self::assertArrayHasKey($c1, $ca->presence());
		self::assertArrayHasKey($a1, $cc->presence());

		$rawB->close();   // the B - C link breaks, and stays down past the TTL
		$this->pump($clock, [$a, $b, $c], 16);
		self::assertArrayNotHasKey($c1, $ca->presence(), 'A declared C down.');
		self::assertArrayNotHasKey($a1, $cc->presence(), 'C declared A down.');

		$this->linkPair($b, $c);   // the link is restored
		$this->pump($clock, [$a, $b, $c], 8);

		self::assertArrayHasKey($c1, $ca->presence(), "A re-learns C's client through B.");
		self::assertArrayHasKey($a1, $cc->presence(), "C re-learns the client of A, which is not its direct peer.");
	}

	public function testPresenceBehindAnIntermediaryConvergesAfterTheDirectLinkFlaps()
	{
		$clock = new TTestMonotonicClock(1000.0);
		[$ca, $a] = $this->makeLineNode('A', $clock);
		[$cb, $b] = $this->makeLineNode('B', $clock);
		[$cc, $c] = $this->makeLineNode('C', $clock);
		[$rawA] = $this->linkPair($a, $b);
		$this->linkPair($b, $c);
		$b1 = $cb->register(new TWebSocketConnection(new TBufferStream(), false));
		$c1 = $cc->register(new TWebSocketConnection(new TBufferStream(), false));
		$this->pump($clock, [$a, $b, $c], 6);
		self::assertArrayHasKey($c1, $ca->presence());

		$rawA->close();   // A's only link breaks past the TTL: A declares B and C down
		$this->pump($clock, [$a, $b, $c], 16);
		self::assertSame([], array_diff_key($ca->presence(), array_flip($ca->getLocalClientIds())), 'A holds no remote client.');

		$this->linkPair($a, $b);
		$this->pump($clock, [$a, $b, $c], 8);

		self::assertArrayHasKey($b1, $ca->presence(), "A re-learns its direct peer's client from the join state.");
		self::assertArrayHasKey($c1, $ca->presence(), "A re-learns the client of C, behind B.");
	}

	public function testPresenceRequestsAreAnsweredAtMostOncePerInterval()
	{
		[$b, , $peer, $raw] = $this->verifiedPeer('A');
		$clock = new TTestMonotonicClock(1000.0);
		$b->setMonotonicClock($clock);
		$b->putPresence('B-1', ['node' => 'B']);
		$read = function () use ($peer, $raw): array {
			$types = [];
			$bytes = $raw->read(1 << 20);
			foreach ($bytes === '' ? [] : $peer->feed($bytes) as $message) {
				$types[] = TWebSocketEnvelope::decode($message)->getType();
			}
			return $types;
		};
		$b->tick();
		$read();   // the join state and the first heartbeat
		$request = fn (array $nodes) => $peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_SYNC, 'A', '', null, null, ['nodes' => $nodes]))->encode());

		$request(['C']);
		$b->tick();
		self::assertNotContains(TWebSocketEnvelope::PRESENCE_SET, $read(), 'A request naming another node is not answered here.');

		$request(['B', 'C']);
		$request(['B']);
		$b->tick();
		$answer = $read();
		self::assertSame(1, count(array_keys($answer, TWebSocketEnvelope::NODE_UP, true)), 'Two requests in one tick get one announce.');
		self::assertContains(TWebSocketEnvelope::PRESENCE_SET, $answer, 'The local clients are re-announced.');

		$request(['B']);
		$b->tick();
		self::assertNotContains(TWebSocketEnvelope::PRESENCE_SET, $read(), 'A request within the interval waits.');
		$clock->advance(TMeshBackplane::ANNOUNCE_INTERVAL);
		$b->tick();
		self::assertContains(TWebSocketEnvelope::PRESENCE_SET, $read(), 'It is answered once the interval passes.');
		$b->close();
	}

	public function testANodeHeardForTheFirstTimeIsAskedForItsPresenceOnce()
	{
		[$b, $spyB, $peer, $raw] = $this->verifiedPeer('A');
		$requests = function () use ($peer, $raw): array {
			$nodes = [];
			$bytes = $raw->read(1 << 20);
			foreach ($bytes === '' ? [] : $peer->feed($bytes) as $message) {
				$envelope = TWebSocketEnvelope::decode($message);
				if ($envelope->getType() === TWebSocketEnvelope::NODE_SYNC) {
					$nodes[] = $envelope->getMeta()['nodes'];
				}
			}
			return $nodes;
		};
		$b->tick();
		self::assertSame([], $requests(), 'The verified direct peer sends its join state; it is not asked.');

		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'C', '', null, null, ['uri' => '']))->encode());
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'D', '', null, 'D-1', ['node' => 'D']))->encode());
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'C', '', null, null, ['uri' => '']))->encode());
		$b->tick();
		self::assertSame([['C', 'D']], $requests(), 'The nodes first heard in one tick are asked for in one request.');

		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'C', '', null, null, ['uri' => '']))->encode());
		$b->tick();
		self::assertSame([], $requests(), 'A node already heard is not asked again.');
		self::assertNotContains(TWebSocketEnvelope::NODE_SYNC, array_map(fn ($e) => $e->getType(), $spyB->received), 'A request is not routed to the coordinator.');
		$b->close();
	}

	public function testAWallClockStepDoesNotDeclareANodeDown()
	{
		[$b, $spyB, $peer] = $this->verifiedPeer('A');
		$wall = new \Prado\Util\Clock\TMockClock();
		$wall->setMicrotime(1000.0);
		$b->setClock($wall);
		$peer->send((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, 'A', '', null, null, ['uri' => '']))->encode());
		$b->tick();

		$wall->setMicrotime(1000.0 + 3600);   // the wall clock steps forward an hour
		$b->tick();

		self::assertSame([], $spyB->droppedNodes, 'A wall-clock step is not a silence.');
		self::assertSame(1, $b->getPeerCount());
		$b->close();
	}

	public function testPeerPatternLimitsWhichAnnouncedPeersAreDialed()
	{
		$mesh = new RecordingMesh();
		$mesh->setPeerPattern('#^tcp://10\.0\.#');
		$mesh->setCluster(new SpyCluster('B'));

		$this->feedGossip($mesh, 'tcp://remote:1', ['tcp://10.0.0.9:1', 'tcp://169.254.169.254:80', 'tcp://10.0.0.9:1']);
		self::assertSame(['tcp://10.0.0.9:1'], $mesh->dialed, 'Only an announced URI matching the pattern is dialed.');
		self::assertSame('#^tcp://10\.0\.#', $mesh->getPeerPattern());
		self::assertSame('', (new TMeshBackplane())->getPeerPattern(), 'Any announced URI is dialed by default.');

		$this->expectException(\Prado\Exceptions\TInvalidDataValueException::class);
		$mesh->setPeerPattern('#unclosed');
	}

	public function testSslContextRoundTrips()
	{
		$mesh = new TMeshBackplane();
		self::assertSame([], $mesh->getSslContext());
		self::assertSame($mesh, $mesh->setSslContext(['cafile' => '/etc/cluster-ca.pem']));
		self::assertSame(['cafile' => '/etc/cluster-ca.pem'], $mesh->getSslContext());
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
