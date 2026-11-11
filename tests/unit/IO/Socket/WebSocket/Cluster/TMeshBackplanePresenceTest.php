<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Stream\TBufferStream;
use Prado\Prado;
use Prado\Test\Unit\IO\Socket\WebSocket\TTestMonotonicClock;
use Prado\Util\Log\TLogger;

/** A coordinator that records the presence changes its backplane delivers, in order. */
class PresenceRecordingCluster extends TWebSocketCluster
{
	/** @var array<int, array{0: string, 1: ?string}> The type and client id of each presence change delivered. */
	public array $changes = [];

	public function receiveEnvelope(TWebSocketEnvelope $envelope): void
	{
		if (in_array($envelope->getType(), [TWebSocketEnvelope::PRESENCE_SET, TWebSocketEnvelope::PRESENCE_DROP], true)) {
			$this->changes[] = [$envelope->getType(), $envelope->getClientId()];
		}
		parent::receiveEnvelope($envelope);
	}

	/** @var string[] The nodes whose whole presence the backplane dropped. */
	public array $droppedNodes = [];

	public function dropNodePresence(string $node): void
	{
		$this->droppedNodes[] = $node;
		parent::dropNodePresence($node);
	}
}

/**
 * Presence across mesh links that drop and return: a node's full presence replaces what the other
 * nodes hold for it, so a change made while a link was down is not lost, and a 1.1.0 node is sent
 * nothing it cannot take.
 */
class TMeshBackplanePresenceTest extends TestCase
{
	private const SECRET = 'shh';

	private TTestMonotonicClock $clock;

	protected function setUp(): void
	{
		$this->clock = new TTestMonotonicClock(1000.0);
	}

	/** @return array{0: PresenceRecordingCluster, 1: TMeshBackplane} A coordinator on a secret-gated mesh backplane. */
	private function node(string $id): array
	{
		$mesh = new TMeshBackplane();
		$mesh->setSecret(self::SECRET);
		$mesh->setMonotonicClock($this->clock);
		return [new PresenceRecordingCluster($id, $mesh), $mesh];
	}

	/**
	 * Links two nodes with a socket pair; `$a` is the dialer.
	 * @param TMeshBackplane $a
	 * @param TMeshBackplane $b
	 * @return array{0: TSocketStream, 1: TSocketStream} The dialer's and the acceptor's transports.
	 */
	private function link(TMeshBackplane $a, TMeshBackplane $b): array
	{
		[$rawA, $rawB] = TSocketStream::pair();
		$rawA->setBlocking(false);
		$rawB->setBlocking(false);
		$a->addPeer(new TWebSocketConnection($rawA, true), $rawA);
		$b->addPeer(new TWebSocketConnection($rawB, false), $rawB);
		return [$rawA, $rawB];
	}

	/** Ticks the nodes in turn until their exchanges settle; the clock stands still. */
	private function pump(TMeshBackplane ...$nodes): void
	{
		for ($i = 0; $i < 12; $i++) {
			foreach ($nodes as $node) {
				$node->tick();
			}
		}
	}

	/** Lets time pass in steps, ticking the nodes at each. */
	private function pass(float $seconds, TMeshBackplane ...$nodes): void
	{
		for ($elapsed = 0.0; $elapsed < $seconds; $elapsed += 0.5) {
			$this->clock->advance(0.5);
			foreach ($nodes as $node) {
				$node->tick();
			}
		}
	}

	private function client(): TWebSocketConnection
	{
		return new TWebSocketConnection(new TBufferStream(), false);
	}

	/** @return array<int, array{0: string, 1: ?string}> The presence drops in a list of recorded changes. */
	private function drops(array $changes): array
	{
		return array_values(array_filter($changes, fn (array $change) => $change[0] === TWebSocketEnvelope::PRESENCE_DROP));
	}

	public function testALinkFlapShorterThanTheTtlForgetsAClientThatLeftMeanwhile()
	{
		[$a, $ma] = $this->node('A');
		[$b, $mb] = $this->node('B');
		[$rawA, $rawB] = $this->link($ma, $mb);
		$this->pump($ma, $mb);
		$left = $b->register($this->client());
		$stays = $b->register($this->client());
		$this->pump($ma, $mb);
		self::assertSame([$left, $stays], array_keys($a->presence()));

		$rawA->close();   // the link drops: a reset, a full send buffer, a malformed frame
		$rawB->close();
		$this->pump($ma, $mb);
		self::assertSame(0, $ma->getPeerCount());
		$this->clock->advance(3);
		$b->unregister($left);   // flooded to no link
		$joined = $b->register($this->client());
		$this->clock->advance(3);
		$a->changes = [];
		$this->link($ma, $mb);   // re-dialed inside the TTL
		$this->pump($ma, $mb);

		self::assertSame(1, $ma->getPeerCount());
		self::assertSame([$stays, $joined], array_keys($a->presence()), 'The full presence of the returning peer replaces what was held for it.');
		self::assertFalse($a->sendToClient($left, 'x'), 'The departed client is unknown again.');
		self::assertSame([[TWebSocketEnvelope::PRESENCE_DROP, $left]], $this->drops($a->changes), 'Only the client that left is removed: one still present is never dropped in between.');
		self::assertSame([], $a->droppedNodes, 'The node is not dropped as a whole.');

		$this->pass(3 * $ma->getNodeTtl(), $ma, $mb);
		self::assertSame([$stays, $joined], array_keys($a->presence()), 'Heartbeats change nothing.');
	}

	public function testALinkFlapBehindAnIntermediaryConvergesOnBothSides()
	{
		[$a, $ma] = $this->node('A');
		[$b, $mb] = $this->node('B');
		[$c, $mc] = $this->node('C');
		$this->link($ma, $mb);
		[$rawB, $rawC] = $this->link($mb, $mc);   // a line: A - B - C
		$this->pump($ma, $mb, $mc);
		$oldA = $a->register($this->client());
		$oldC = $c->register($this->client());
		$this->pump($ma, $mb, $mc);
		self::assertArrayHasKey($oldA, $c->presence());
		self::assertArrayHasKey($oldC, $a->presence());

		$rawB->close();   // B - C flaps
		$rawC->close();
		$this->pump($ma, $mb, $mc);
		$newA = $a->register($this->client());
		$a->unregister($oldA);
		$newC = $c->register($this->client());
		$c->unregister($oldC);
		$this->pump($ma, $mb, $mc);
		self::assertArrayHasKey($oldA, $c->presence(), 'C could not hear that the client of A left.');
		$this->clock->advance(5);
		$this->link($mb, $mc);
		$this->pass(2 * TMeshBackplane::ANNOUNCE_INTERVAL, $ma, $mb, $mc);

		self::assertSame([$newA], array_keys(array_diff_key($c->presence(), [$newC => true])), 'C replaces what it held for A, which is behind B.');
		self::assertSame([$newC], array_keys(array_diff_key($a->presence(), [$newA => true])), 'A replaces what it held for C, which is behind B.');
		self::assertSame([$newA, $newC], array_keys($b->presence()), 'The intermediary holds both.');

		$this->pass(3 * $ma->getNodeTtl(), $ma, $mb, $mc);
		self::assertSame([$newA], array_keys(array_diff_key($c->presence(), [$newC => true])), 'The mirrors stay converged.');
		self::assertSame([], array_merge($a->droppedNodes, $b->droppedNodes, $c->droppedNodes), 'No node was declared down.');
	}

	public function testALinkUpAsksThePeerForTheNodesHeardButNotLinked()
	{
		[, $mesh] = $this->node('B');
		$a = MeshTestPeer::acceptedBy($mesh, 'A', self::SECRET);
		self::assertNotContains(TWebSocketEnvelope::NODE_SYNC, $a->readTypes(), 'A node that has heard no other node asks for none.');
		$a->announce([], 'D');   // A relays the heartbeat of D, which is behind it
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'E', '', null, 'E-1', ['node' => 'E']));
		$mesh->tick();
		$a->read();

		$c = MeshTestPeer::acceptedBy($mesh, 'C', self::SECRET);
		$requests = array_values(array_filter($c->read(), fn ($envelope) => $envelope->getType() === TWebSocketEnvelope::NODE_SYNC));
		self::assertCount(1, $requests, 'The new peer is asked once.');
		self::assertSame(['D', 'E'], $requests[0]->getMeta()['nodes'], 'It is asked for the nodes heard and not linked: neither A, which is linked, nor itself.');
		self::assertNotContains(TWebSocketEnvelope::NODE_SYNC, $a->readTypes(), 'The request goes over the new link only.');
		$mesh->close();
	}

	public function testAFullPresenceIsFloodedToEveryLinkWhenALinkIsVerified()
	{
		[$b, $mesh] = $this->node('B');
		$client = $b->register($this->client());
		$a = MeshTestPeer::acceptedBy($mesh, 'A', self::SECRET);
		$first = $a->read();
		$types = [TWebSocketEnvelope::NODE_UP, TWebSocketEnvelope::PRESENCE_SET, TWebSocketEnvelope::NODE_UP];
		self::assertSame([...$types, TWebSocketEnvelope::NODE_UP], array_map(fn ($envelope) => $envelope->getType(), $first), 'An opening announce, the clients, and a closing announce; then the first heartbeat.');
		self::assertSame(1, $first[3]->getMeta()['generation'], 'A heartbeat carries the generation.');
		self::assertArrayNotHasKey('presence', $first[3]->getMeta(), 'A heartbeat neither opens nor closes a full presence.');
		self::assertSame('begin', $first[0]->getMeta()['presence']);
		self::assertSame('end', $first[2]->getMeta()['presence']);
		$generation = $first[0]->getMeta()['generation'];
		self::assertSame(1, $generation, 'The first full presence is generation 1.');
		self::assertSame($generation, $first[2]->getMeta()['generation']);
		self::assertSame((string) $generation, $first[1]->getPayload(), 'A presence envelope carries the generation as its payload.');
		self::assertSame($client, $first[1]->getClientId());
		self::assertSame(['node' => 'B'], $first[1]->getMeta(), 'The presence metadata is the client\'s own: nothing is added to it.');

		$c = MeshTestPeer::acceptedBy($mesh, 'C', self::SECRET);
		$second = $a->read();
		self::assertSame($types, array_map(fn ($envelope) => $envelope->getType(), $second), 'The linked peer receives the full presence a new link causes, for the nodes behind it.');
		self::assertSame(2, $second[0]->getMeta()['generation'], 'Each full presence has its own generation.');
		self::assertSame(2, $c->read()[0]->getMeta()['generation']);

		$later = $b->register($this->client());
		$b->unregister($later);
		$changes = $a->read();
		self::assertSame([TWebSocketEnvelope::PRESENCE_SET, TWebSocketEnvelope::PRESENCE_DROP], array_map(fn ($envelope) => $envelope->getType(), $changes));
		self::assertSame(['2', '2'], array_map(fn ($envelope) => $envelope->getPayload(), $changes), 'Each later change carries the generation it follows.');
		$mesh->close();
	}

	public function testTheClosingAnnounceDropsOnlyTheClientsItsGenerationDidNotSet()
	{
		[$b, $mesh] = $this->node('B');
		$a = MeshTestPeer::acceptedBy($mesh, 'A', self::SECRET);
		$set = fn (string $client, string $generation) => $a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'A', $generation, null, $client, ['node' => 'A']));
		$a->announce(['generation' => 1, 'presence' => 'begin']);
		$set('A-1', '1');
		$set('A-2', '1');
		$a->announce(['generation' => 1, 'presence' => 'end']);
		$mesh->tick();
		self::assertSame(['A-1', 'A-2'], array_keys($b->presence()));

		$b->changes = [];
		$a->announce(['generation' => 2, 'presence' => 'begin']);
		$mesh->tick();
		self::assertSame(['A-1', 'A-2'], array_keys($b->presence()), 'The opening announce removes nothing.');
		$set('A-2', '2');
		$set('A-3', '2');
		$mesh->tick();
		self::assertSame(['A-1', 'A-2', 'A-3'], array_keys($b->presence()), 'Nothing is removed before the full presence is complete.');
		$a->announce(['generation' => 2, 'presence' => 'end']);
		$mesh->tick();

		self::assertSame(['A-2', 'A-3'], array_keys($b->presence()), 'The client the generation did not set has left.');
		self::assertSame([[TWebSocketEnvelope::PRESENCE_DROP, 'A-1']], $this->drops($b->changes));
		$mesh->close();
	}

	public function testAPresenceChangeOfAnOlderGenerationIsNotApplied()
	{
		[$b, $mesh] = $this->node('B');
		$a = MeshTestPeer::acceptedBy($mesh, 'A', self::SECRET);
		$a->announce(['generation' => 5, 'presence' => 'begin']);
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'A', '5', null, 'A-now', ['node' => 'A']));
		$a->announce(['generation' => 5, 'presence' => 'end']);
		$mesh->tick();

		// Envelopes sent before that full presence, which reach B late over a slower path.
		$a->announce(['generation' => 4, 'presence' => 'end']);
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'A', '4', null, 'A-gone', ['node' => 'A']));
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_DROP, 'A', '4', null, 'A-now'));
		$a->announce(['generation' => 4]);
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'A', '4', null, 'A-gone-too', ['node' => 'A']));
		$mesh->tick();
		self::assertSame(['A-now'], array_keys($b->presence()), 'Neither an announce nor a change older than the full presence adds or removes a client, or makes an older generation current again.');

		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'A', '5', null, 'A-new', ['node' => 'A']));
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_DROP, 'A', '5', null, 'A-now'));
		$mesh->tick();
		self::assertSame(['A-new'], array_keys($b->presence()), 'A change of the current generation applies.');

		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'A', '6', null, 'A-next', ['node' => 'A']));
		$mesh->tick();
		self::assertSame(['A-new', 'A-next'], array_keys($b->presence()), 'A change that runs ahead of its full presence applies, and removes nothing.');
		$a->announce(['generation' => 6, 'presence' => 'end']);
		$mesh->tick();
		self::assertSame(['A-next'], array_keys($b->presence()), 'It survives the closing announce of its generation.');
		$mesh->close();
	}

	public function testARelayedFullPresenceReplacesWhatIsHeldForADistantNode()
	{
		[$b, $mesh] = $this->node('B');
		$a = MeshTestPeer::acceptedBy($mesh, 'A', self::SECRET);
		$c = MeshTestPeer::acceptedBy($mesh, 'C', self::SECRET);
		$a->read();
		$c->read();
		$a->announce(['generation' => 1, 'presence' => 'begin'], 'D');   // A relays the full presence of D
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'D', '1', null, 'D-1', ['node' => 'D']));
		$a->announce(['generation' => 1, 'presence' => 'end'], 'D');
		$mesh->tick();
		self::assertSame(['D-1'], array_keys($b->presence()));
		self::assertNotContains(TWebSocketEnvelope::NODE_SYNC, $c->readTypes(), 'A node first heard through the opening of its full presence is not asked for it.');

		$a->announce(['generation' => 2, 'presence' => 'begin'], 'D');
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'D', '2', null, 'D-2', ['node' => 'D']));
		$a->announce(['generation' => 2, 'presence' => 'end'], 'D');
		$mesh->tick();
		self::assertSame(['D-2'], array_keys($b->presence()), 'The full presence of a node behind the peer is authoritative too.');
		$relayed = array_map(fn ($envelope) => [$envelope->getType(), $envelope->getPayload(), $envelope->getMeta()['presence'] ?? null], $c->read());
		self::assertSame([
			[TWebSocketEnvelope::NODE_UP, '', 'begin'],
			[TWebSocketEnvelope::PRESENCE_SET, '2', null],
			[TWebSocketEnvelope::NODE_UP, '', 'end'],
		], $relayed, 'It is relayed whole and in order, generation included.');
		$mesh->close();
	}

	public function testANewIncarnationCountsItsGenerationsFromTheStart()
	{
		[$b, $mesh] = $this->node('B');
		$a = MeshTestPeer::acceptedBy($mesh, 'A', self::SECRET);
		$a->announce(['incarnation' => 'one', 'generation' => 7, 'presence' => 'begin']);
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'A', '7', null, 'A-old', ['node' => 'A']));
		$a->announce(['incarnation' => 'one', 'generation' => 7, 'presence' => 'end']);
		$mesh->tick();
		self::assertSame(['A-old'], array_keys($b->presence()));

		$a->announce(['incarnation' => 'two', 'generation' => 1, 'presence' => 'begin']);   // A restarted
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'A', '1', null, 'A-new', ['node' => 'A']));
		$a->announce(['incarnation' => 'two', 'generation' => 1, 'presence' => 'end']);
		$mesh->tick();
		self::assertSame(['A-new'], array_keys($b->presence()), 'The restarted node\'s first generation is not taken for an old one.');
		$mesh->close();
	}

	public function testANodeDeclaredDownStartsAfreshWhenItReturns()
	{
		[$b, $mesh] = $this->node('B');
		$mesh->setNodeTtl(3);
		$a = MeshTestPeer::acceptedBy($mesh, 'A', self::SECRET);
		$a->announce(['incarnation' => 'd', 'generation' => 9, 'presence' => 'begin'], 'D');
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'D', '9', null, 'D-1', ['node' => 'D']));
		$a->announce(['incarnation' => 'd', 'generation' => 9, 'presence' => 'end'], 'D');
		$mesh->tick();
		self::assertSame(['D-1'], array_keys($b->presence()));

		for ($i = 0; $i < 10; $i++) {   // D falls silent past the TTL while A keeps talking
			$this->clock->advance(1);
			$a->announce();
			$mesh->tick();
		}
		self::assertSame([], array_keys($b->presence()), 'D is declared down.');
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'D', '1', null, 'D-2', ['node' => 'D']));   // D returns, restarted: its presence is of generation 1 again
		$mesh->tick();
		self::assertSame(['D-2'], array_keys($b->presence()), 'What was heard of D before it was declared down does not outlive it.');
		$mesh->close();
	}

	public function testA110NodesPresenceIsAddedAndNeverSwept()
	{
		[$b, $mesh] = $this->node('B');
		$old = MeshTestPeer::acceptedBy($mesh, 'OLD', self::SECRET);
		// The join state of a 1.1.0 node: its presence, then its announce, neither with a generation.
		$old->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'OLD', '', null, 'OLD-1', ['node' => 'OLD']));
		$old->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'OLD', '', null, 'OLD-2', ['node' => 'OLD']));
		$old->announce(['uri' => 'tcp://10.0.0.9:1']);
		$mesh->tick();
		self::assertSame(['OLD-1', 'OLD-2'], array_keys($b->presence()), 'The announce that follows the presence of a 1.1.0 node removes none of it.');

		for ($i = 0; $i < 5; $i++) {
			$this->clock->advance(5);
			$old->announce(['uri' => 'tcp://10.0.0.9:1']);   // its heartbeats
			$mesh->tick();
		}
		$old->send(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_DROP, 'OLD', '', null, 'OLD-1'));
		$mesh->tick();
		self::assertSame(['OLD-2'], array_keys($b->presence()), 'Its presence changes apply as they arrive.');
		self::assertSame([], $b->droppedNodes);
		$mesh->close();
	}

	public function testA110NodeIsSentOnlyEnvelopesItTakes()
	{
		[$b, $mesh] = $this->node('B');
		$mesh->setAdvertise('tcp://10.0.0.2:8080');
		$b->register($this->client(), ['user' => 'alice']);
		$a = MeshTestPeer::acceptedBy($mesh, 'A', self::SECRET);
		$a->announce([], 'D');
		$mesh->tick();
		MeshTestPeer::acceptedBy($mesh, 'C', self::SECRET);
		$b->register($this->client(), ['user' => 'bob']);
		$mesh->tick();

		// What 1.1.0 does with each envelope: it decodes any type, routes the five it knows to its
		// coordinator, reads `uri` from an announce, and relays the rest.  It reads the payload of
		// a publish, broadcast or direct only, and stores a presence envelope's metadata as given.
		foreach ($a->read() as $envelope) {
			self::assertContains($envelope->getType(), [TWebSocketEnvelope::NODE_UP, TWebSocketEnvelope::PRESENCE_SET, TWebSocketEnvelope::NODE_SYNC], 'Only types a 1.1.0 node handles or relays are sent.');
			if ($envelope->getType() === TWebSocketEnvelope::NODE_UP) {
				self::assertSame('tcp://10.0.0.2:8080', $envelope->getMeta()['uri'], 'The URI is where 1.1.0 reads it.');
			}
			if ($envelope->getType() === TWebSocketEnvelope::PRESENCE_SET) {
				self::assertSame(['user', 'node'], array_keys($envelope->getMeta()), 'The presence metadata 1.1.0 stores is the client\'s own.');
				self::assertNotNull($envelope->getClientId());
			}
		}
		$mesh->close();
	}

	public function testAFullPresenceThatLoopsBackIsNotTakenForANodeIdClash()
	{
		Prado::getLogger()->deleteLogs(null, TMeshBackplane::class);
		[$a, $ma] = $this->node('A');
		[, $mb] = $this->node('B');
		[, $mc] = $this->node('C');
		$a->register($this->client());
		$this->link($ma, $mb);
		$this->link($mb, $mc);
		$this->link($mc, $ma);   // a triangle: what A floods to B returns to A through C
		$this->pump($ma, $mb, $mc);

		$clashes = array_filter(Prado::getLogger()->getLogs(TLogger::WARNING, TMeshBackplane::class), fn ($log) => str_contains($log[0], "using this node's id"));
		self::assertSame([], $clashes, 'A node knows the envelopes of its own full presence when they return.');
	}

	public function testUnencodablePresenceMetadataIsRefusedToTheCallerAndTouchesNoLink()
	{
		[$a, $ma] = $this->node('A');
		[$b, $mb] = $this->node('B');
		[$c, $mc] = $this->node('C');
		$this->link($ma, $mb);
		$this->link($ma, $mc);
		$this->pump($ma, $mb, $mc);
		self::assertSame(2, $ma->getPeerCount());

		try {
			$a->register($this->client(), ['score' => NAN]);
			self::fail('Metadata that cannot be encoded is refused.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_envelope_unencodable', $e->getErrorCode());
		}
		self::assertSame(2, $ma->getPeerCount(), 'No link is dropped for it.');
		self::assertSame(0, $a->getLocalClientCount(), 'The client is not registered.');
		self::assertSame([], $a->presence());

		$good = $a->register($this->client(), ['score' => 1.5]);
		[$d, $md] = $this->node('D');
		Prado::getLogger()->deleteLogs(null, TMeshBackplane::class);
		$this->link($ma, $md);   // a later link: the full presence holds nothing that cannot be sent
		$this->pump($ma, $mb, $mc, $md);
		self::assertSame(3, $ma->getPeerCount());
		self::assertArrayHasKey($good, $d->presence());
		self::assertArrayHasKey($good, $b->presence());
		self::assertSame([], Prado::getLogger()->getLogs(TLogger::WARNING, TMeshBackplane::class), 'The refused metadata was not kept: every envelope of the full presence is sent.');
	}

	public function testAnAnnounceWhoseUriIsNotAStringIsNoUri()
	{
		[$b, $mesh] = $this->node('B');
		$a = MeshTestPeer::acceptedBy($mesh, 'A', self::SECRET);
		$sink = new TBufferStream();
		$b->register(new TWebSocketConnection($sink, false));
		$a->announce(['uri' => ['tcp://10.0.0.9:1']], 'X');
		$a->announce(['uri' => 7], 'Y');
		$a->send(new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'X', 'after'));
		// PRADO's error handler turns a warning, as an array-to-string conversion raises, into an exception.
		set_error_handler(function (int $level, string $message, string $file, int $line): bool {
			throw new \ErrorException($message, 0, $level, $file, $line);
		});
		try {
			$mesh->tick();
		} finally {
			restore_error_handler();
		}
		self::assertSame(1, $mesh->getPeerCount(), 'The peer is kept.');
		self::assertSame(0, $mesh->getPendingCount(), 'Nothing is dialed for it.');
		self::assertSame(0, $mesh->getResolvingCount());
		self::assertStringContainsString('after', (string) $sink, 'The envelope after the announces is still routed.');
		$mesh->close();
	}
}
