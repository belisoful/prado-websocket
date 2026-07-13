<?php

use Prado\IO\Socket\WebSocket\Cluster\IWebSocketBackplane;
use Prado\IO\Socket\WebSocket\Cluster\IWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Stream\TBufferStream;
use Prado\TComponent;

/** Captures every backplane call so the coordinator's fan-out can be asserted. */
class SpyBackplane extends TComponent implements IWebSocketBackplane
{
	public ?IWebSocketCluster $cluster = null;
	/** @var TWebSocketEnvelope[] */
	public array $published = [];
	public array $subscribed = [];
	public array $unsubscribed = [];
	public array $presenceSet = [];
	public array $presenceDropped = [];

	public function setCluster(IWebSocketCluster $cluster): void
	{
		$this->cluster = $cluster;
	}

	public function open(): void
	{
	}

	public function close(): void
	{
	}

	public function tick(): void
	{
	}

	public function getSources(): array
	{
		return [];
	}

	public function publish(TWebSocketEnvelope $envelope): void
	{
		$this->published[] = $envelope;
	}

	public function subscribe(string $channel): void
	{
		$this->subscribed[] = $channel;
	}

	public function unsubscribe(string $channel): void
	{
		$this->unsubscribed[] = $channel;
	}

	public function putPresence(string $clientId, array $meta): void
	{
		$this->presenceSet[$clientId] = $meta;
	}

	public function dropPresence(string $clientId): void
	{
		$this->presenceDropped[] = $clientId;
	}
}

class TWebSocketClusterTest extends PHPUnit\Framework\TestCase
{
	private SpyBackplane $spy;
	private TWebSocketCluster $cluster;

	protected function setUp(): void
	{
		$this->spy = new SpyBackplane();
		$this->cluster = new TWebSocketCluster('nodeA', $this->spy);
	}

	/** @return array{0: TWebSocketConnection, 1: TBufferStream} A server-side connection and its sink. */
	private function makeConnection(): array
	{
		$stream = new TBufferStream();
		return [new TWebSocketConnection($stream, false), $stream];
	}

	public function testSetBackplaneBindsTheCoordinator()
	{
		self::assertSame($this->cluster, $this->spy->cluster, 'The backplane is bound to its coordinator.');
		self::assertSame('nodeA', $this->cluster->getNodeId());
	}

	public function testRegisterAssignsUniqueIdsAndAnnouncesPresence()
	{
		[$a] = $this->makeConnection();
		[$b] = $this->makeConnection();
		$idA = $this->cluster->register($a, ['user' => 'alice']);
		$idB = $this->cluster->register($b);

		self::assertNotSame($idA, $idB, 'Each client gets a distinct id.');
		self::assertStringStartsWith('nodeA-', $idA, 'Client ids are node-scoped.');
		self::assertTrue($this->cluster->hasLocalClient($idA));
		self::assertSame($idA, $this->cluster->getClientId($a));
		self::assertSame('nodeA', $this->spy->presenceSet[$idA]['node'], 'Presence carries the node id.');
		self::assertSame('alice', $this->spy->presenceSet[$idA]['user'], 'Presence carries the metadata.');
	}

	public function testSubscribeDeclaresChannelInterestOnce()
	{
		[$a] = $this->makeConnection();
		[$b] = $this->makeConnection();
		$idA = $this->cluster->register($a);
		$idB = $this->cluster->register($b);

		$this->cluster->subscribe($idA, 'news');
		$this->cluster->subscribe($idB, 'news');
		self::assertSame(['news'], $this->spy->subscribed, 'Only the first local subscriber declares interest.');

		$this->cluster->unsubscribe($idA, 'news');
		self::assertSame([], $this->spy->unsubscribed, 'Interest holds while a local subscriber remains.');
		$this->cluster->unsubscribe($idB, 'news');
		self::assertSame(['news'], $this->spy->unsubscribed, 'The last local subscriber withdraws interest.');
	}

	public function testPublishReachesLocalSubscribersAndCluster()
	{
		[$a, $sa] = $this->makeConnection();
		[$b, $sb] = $this->makeConnection();
		$idA = $this->cluster->register($a);
		$this->cluster->register($b);
		$this->cluster->subscribe($idA, 'news');

		$this->cluster->publish('news', 'hello');
		self::assertGreaterThan(0, $sa->getSize(), 'A subscriber receives the publish.');
		self::assertSame(0, $sb->getSize(), 'A non-subscriber receives nothing.');

		self::assertCount(1, $this->spy->published, 'The publish fans out to the cluster once.');
		$env = $this->spy->published[0];
		self::assertSame(TWebSocketEnvelope::PUBLISH, $env->getType());
		self::assertSame('news', $env->getChannel());
		self::assertSame('hello', $env->getPayload());
		self::assertSame('nodeA', $env->getOriginNode());
	}

	public function testBroadcastReachesEveryLocalClient()
	{
		[$a, $sa] = $this->makeConnection();
		[$b, $sb] = $this->makeConnection();
		$this->cluster->register($a);
		$this->cluster->register($b);

		$this->cluster->broadcast('all');
		self::assertGreaterThan(0, $sa->getSize());
		self::assertGreaterThan(0, $sb->getSize());
		self::assertSame(TWebSocketEnvelope::BROADCAST, $this->spy->published[0]->getType());
	}

	public function testDirectSendIsLocalWhenPresentAndRoutedWhenRemote()
	{
		[$a, $sa] = $this->makeConnection();
		$idA = $this->cluster->register($a);

		self::assertTrue($this->cluster->sendToClient($idA, 'hi'), 'A local client is reachable.');
		self::assertGreaterThan(0, $sa->getSize(), 'A local direct send is delivered directly.');
		self::assertCount(0, $this->spy->published, 'A local direct send is not put on the backplane.');

		self::assertFalse($this->cluster->sendToClient('nodeB-7', 'yo'), 'An unknown client is reported absent.');
		self::assertCount(1, $this->spy->published, 'A remote direct send is routed through the backplane.');
		self::assertSame(TWebSocketEnvelope::DIRECT, $this->spy->published[0]->getType());
		self::assertSame('nodeB-7', $this->spy->published[0]->getClientId());
	}

	public function testReceivePublishDeliversLocallyWithoutRepublishing()
	{
		[$a, $sa] = $this->makeConnection();
		$idA = $this->cluster->register($a);
		$this->cluster->subscribe($idA, 'news');

		$this->cluster->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'nodeB', 'remote', 'news'));
		self::assertGreaterThan(0, $sa->getSize(), 'A remote publish reaches the local subscriber.');
		self::assertCount(0, $this->spy->published, 'Inbound traffic is never relayed back to the cluster.');
	}

	public function testReceiveDropsOwnEcho()
	{
		[$a, $sa] = $this->makeConnection();
		$idA = $this->cluster->register($a);
		$this->cluster->subscribe($idA, 'news');

		$this->cluster->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'nodeA', 'echo', 'news'));
		self::assertSame(0, $sa->getSize(), 'An envelope from this node is ignored.');
	}

	public function testReceiveDirectTargetsLocalClientOnly()
	{
		[$a, $sa] = $this->makeConnection();
		$idA = $this->cluster->register($a);

		$this->cluster->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::DIRECT, 'nodeB', 'for-a', null, $idA));
		self::assertGreaterThan(0, $sa->getSize(), 'A direct envelope for a local client is delivered.');

		$before = $sa->getSize();
		$this->cluster->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::DIRECT, 'nodeB', 'elsewhere', null, 'nodeC-1'));
		self::assertSame($before, $sa->getSize(), 'A direct envelope for a foreign client is not delivered here.');
	}

	public function testPresenceMirrorTracksRemoteEnvelopes()
	{
		$this->cluster->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'nodeB', '', null, 'nodeB-1', ['node' => 'nodeB', 'user' => 'bob']));
		self::assertSame('bob', $this->cluster->presence()['nodeB-1']['user'], 'A remote presence joins the mirror.');

		$this->cluster->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_DROP, 'nodeB', '', null, 'nodeB-1'));
		self::assertArrayNotHasKey('nodeB-1', $this->cluster->presence(), 'A remote departure leaves the mirror.');
	}

	public function testUnregisterCleansUpSubscriptionsAndPresence()
	{
		[$a] = $this->makeConnection();
		$idA = $this->cluster->register($a);
		$this->cluster->subscribe($idA, 'news');

		$this->cluster->unregister($a);
		self::assertFalse($this->cluster->hasLocalClient($idA));
		self::assertArrayNotHasKey($idA, $this->cluster->presence());
		self::assertContains($idA, $this->spy->presenceDropped, 'Departure is announced to the cluster.');
		self::assertContains('news', $this->spy->unsubscribed, 'The vacated channel withdraws interest.');
	}

	public function testEnvelopeEncodeRoundTrip()
	{
		$env = new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'nodeA', 'body', 'chan', 'nodeA-3', ['k' => 'v'], 'id-1');
		$copy = TWebSocketEnvelope::decode($env->encode());
		self::assertNotNull($copy);
		self::assertSame('chan', $copy->getChannel());
		self::assertSame('body', $copy->getPayload());
		self::assertSame('nodeA-3', $copy->getClientId());
		self::assertSame(['k' => 'v'], $copy->getMeta());
		self::assertSame('id-1', $copy->getId());
		self::assertNull(TWebSocketEnvelope::decode('{not json'));
	}

	public function testReadOnlyIntrospectionReportsLocalAndClusterState()
	{
		[$a] = $this->makeConnection();
		[$b] = $this->makeConnection();
		$idA = $this->cluster->register($a, ['user' => 'alice']);
		$idB = $this->cluster->register($b);
		$this->cluster->subscribe($idA, 'news');
		$this->cluster->subscribe($idB, 'news');
		$this->cluster->subscribe($idA, 'sports');

		// A remote client on another node arrives through the presence mirror.
		$this->cluster->receiveEnvelope(new TWebSocketEnvelope(
			TWebSocketEnvelope::PRESENCE_SET,
			'nodeB',
			'',
			null,
			'nodeB-1',
			['node' => 'nodeB'],
		));

		self::assertSame([$idA, $idB], $this->cluster->getLocalClientIds());
		self::assertSame(2, $this->cluster->getLocalClientCount());
		self::assertSame(3, $this->cluster->getClusterClientCount(), 'Two local clients plus one remote.');
		self::assertSame(['nodeA' => 2, 'nodeB' => 1], $this->cluster->getNodes(), 'Per-node counts span the cluster.');
		self::assertSame(['news' => 2, 'sports' => 1], $this->cluster->getChannels());
		self::assertSame([$idA, $idB], $this->cluster->getChannelSubscribers('news'));
		self::assertSame([], $this->cluster->getChannelSubscribers('absent'));
	}

	public function testGetStatsSnapshot()
	{
		[$a] = $this->makeConnection();
		$idA = $this->cluster->register($a);
		$this->cluster->subscribe($idA, 'news');

		$stats = $this->cluster->getStats();
		self::assertSame('nodeA', $stats['node']);
		self::assertSame(SpyBackplane::class, $stats['backplane']);
		self::assertSame(1, $stats['localClients']);
		self::assertSame(1, $stats['clusterClients']);
		self::assertSame(['nodeA' => 1], $stats['nodes']);
		self::assertSame(['news' => 1], $stats['channels']);
	}
}
