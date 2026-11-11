<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\Stream\TBufferStream;
use Prado\Test\Unit\IO\Socket\WebSocket\TTestMonotonicClock;

/** A mesh that records the URIs it is asked to dial and opens no socket. */
class DialRecordingMesh extends TMeshBackplane
{
	/** @var string[] */
	public array $dialed = [];

	public function connectPeer(string $uri): void
	{
		$this->dialed[] = $uri;
	}
}

/**
 * The dialing side of a mesh link: the failures a dial meets in its WebSocket handshake, and the
 * rule that two nodes keep one link.
 */
class TMeshBackplaneDialTest extends TestCase
{
	private const SECRET = 'shh';

	private TTestMonotonicClock $clock;

	/** @var TSocketStream[] The listeners to close. */
	private array $listeners = [];

	/** @var resource[] The accepted sockets to close. */
	private array $accepted = [];

	/** @var TMeshBackplane[] The meshes to close. */
	private array $meshes = [];

	protected function setUp(): void
	{
		$this->clock = new TTestMonotonicClock(1000.0);
	}

	protected function tearDown(): void
	{
		foreach ($this->meshes as $mesh) {
			$mesh->close();
		}
		foreach ($this->accepted as $socket) {
			if (is_resource($socket)) {
				fclose($socket);
			}
		}
		foreach ($this->listeners as $listener) {
			$listener->close();
		}
		$this->meshes = $this->accepted = $this->listeners = [];
	}

	/** @return TMeshBackplane A secret-gated mesh bound to a coordinator, on the test clock. */
	private function mesh(string $node, ?TMeshBackplane $mesh = null): TMeshBackplane
	{
		$this->meshes[] = $mesh ??= new TMeshBackplane();
		$mesh->setSecret(self::SECRET);
		$mesh->setMonotonicClock($this->clock);
		new TWebSocketCluster($node, $mesh);
		return $mesh;
	}

	/**
	 * Dials a loopback listener this test answers by hand, and reads the upgrade request the mesh
	 * sends once its connect completes.
	 * @param TMeshBackplane $mesh
	 * @return array{0: string, 1: resource, 2: array<string, mixed>} The dialed URI, the accepted socket, and the parsed request.
	 */
	private function dial(TMeshBackplane $mesh): array
	{
		$this->listeners[] = $listener = TSocketStream::bind('tcp://127.0.0.1:0', STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);
		$uri = 'tcp://127.0.0.1:' . $listener->getLocalAddress()->getPort();
		$mesh->connectPeer($uri);
		self::assertSame(1, $mesh->getPendingCount());
		$socket = false;
		$request = '';
		for ($i = 0; $i < 2000 && !str_contains($request, "\r\n\r\n"); $i++) {
			$mesh->tick();
			if ($socket === false) {
				$socket = @stream_socket_accept($listener->getResource(), 0);
				if ($socket !== false) {
					stream_set_blocking($socket, false);
					$this->accepted[] = $socket;
				}
			} else {
				$request .= (string) fread($socket, 8192);
			}
			usleep(500);
		}
		self::assertStringContainsString("\r\n\r\n", $request, 'The mesh sends its upgrade request.');
		self::assertSame(1, $mesh->getPendingCount(), 'The dial waits for the answer.');
		return [$uri, $socket, TWebSocketHandshake::parseHttpMessage($request)];
	}

	/**
	 * Writes the listener's answer in full, ticking the mesh as it reads, then ticks until the dial
	 * has settled.
	 * @param resource $socket
	 * @param bool $close Whether the listener hangs up after the bytes.
	 * @param bool $settles Whether the answer ends the dial, so the mesh is ticked until it has.
	 * @param TMeshBackplane $mesh
	 * @param string $bytes
	 */
	private function answer(TMeshBackplane $mesh, $socket, string $bytes, bool $close = false, bool $settles = true): void
	{
		for ($i = 0; $i < 5000 && $bytes !== '' && $mesh->getPendingCount() > 0; $i++) {
			$written = @fwrite($socket, $bytes);
			$bytes = substr($bytes, (int) $written);
			$mesh->tick();
		}
		if ($close) {
			fclose($socket);
		}
		for ($i = 0; $i < ($settles ? 4000 : 20) && $mesh->getPendingCount() > 0; $i++) {
			$mesh->tick();
			usleep(500);
		}
	}

	/** Asserts a dial failed, left no link, and freed its URI to be dialed again. */
	private function assertDialFailed(TMeshBackplane $mesh, string $uri, string $message): void
	{
		self::assertSame(0, $mesh->getPendingCount(), $message);
		self::assertSame(0, $mesh->getPeerCount(), 'A failed dial adds no link.');
		$mesh->connectPeer($uri);
		self::assertSame(1, $mesh->getPendingCount(), 'The URI of a failed dial can be dialed again.');
	}

	public function testTheUpgradeRequestProvesTheSecretOverItsKey()
	{
		$mesh = $this->mesh('A');
		$mesh->setHost('mesh.internal');
		$mesh->setPath('/peers');
		[, , $request] = $this->dial($mesh);

		self::assertSame('GET', $request['method']);
		self::assertSame('/peers', $request['target']);
		self::assertSame('mesh.internal', $request['headers']['host']);
		$verifier = $this->mesh('B');
		self::assertTrue($verifier->authenticate($request['headers']), 'A node with the secret accepts the proof.');
		$stranger = new TMeshBackplane();
		$stranger->setSecret('another secret');
		self::assertFalse($stranger->authenticate($request['headers']), 'A node with another secret refuses it.');
	}

	public function testADialAnsweredWithoutA101Fails()
	{
		$mesh = $this->mesh('A');
		[$uri, $socket] = $this->dial($mesh);
		$this->answer($mesh, $socket, TWebSocketHandshake::buildRejection(403, 'Forbidden'));
		$this->assertDialFailed($mesh, $uri, 'A refusal fails the dial.');
	}

	public function testADialAnsweredWithAWrongAcceptKeyFails()
	{
		$mesh = $this->mesh('A');
		[$uri, $socket] = $this->dial($mesh);
		$this->answer($mesh, $socket, TWebSocketHandshake::buildServerResponse(TWebSocketHandshake::generateKey()));   // the accept of another key
		$this->assertDialFailed($mesh, $uri, 'A 101 that does not answer the key sent fails the dial.');
	}

	public function testADialAnsweredWithAnEndlessHeadFails()
	{
		$mesh = $this->mesh('A');
		[$uri, $socket] = $this->dial($mesh);
		$this->answer($mesh, $socket, "HTTP/1.1 101 Switching Protocols\r\nX-Padding: " . str_repeat('a', 4 * TMeshBackplane::READ_CHUNK));
		$this->assertDialFailed($mesh, $uri, 'A response head past the read chunk without an end fails the dial.');
	}

	public function testADialWhosePeerHangsUpMidHandshakeFails()
	{
		$mesh = $this->mesh('A');
		[$uri, $socket] = $this->dial($mesh);
		$this->answer($mesh, $socket, "HTTP/1.1 101 Switching Pro", true);
		$this->assertDialFailed($mesh, $uri, 'A peer that closes before the head is complete fails the dial.');
	}

	public function testADialWhosePeerHangsUpWithoutAWordFails()
	{
		$mesh = $this->mesh('A');
		[$uri, $socket] = $this->dial($mesh);
		$this->answer($mesh, $socket, '', true);
		$this->assertDialFailed($mesh, $uri, 'A peer that closes without answering fails the dial.');
	}

	public function testADialUnansweredAtItsDeadlineFails()
	{
		$mesh = $this->mesh('A');
		[$uri] = $this->dial($mesh);
		$this->clock->advance($mesh->getTimeout() / 2);
		$mesh->tick();
		self::assertSame(1, $mesh->getPendingCount(), 'The dial waits until its deadline.');
		$this->clock->advance($mesh->getTimeout());
		$mesh->tick();
		$this->assertDialFailed($mesh, $uri, 'An upgrade unanswered at the dial timeout fails the dial.');
	}

	public function testADialAnsweredInPiecesWithFramesBehindThe101Links()
	{
		$mesh = $this->mesh('A');
		[$uri, $socket, $request] = $this->dial($mesh);
		$frames = new TBufferStream();
		(new TWebSocketConnection($frames, false))->send((new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_CHALLENGE, 'B', 'nonce-of-b'))->encode());
		$response = TWebSocketHandshake::buildServerResponse($request['headers']['sec-websocket-key']) . (string) $frames;
		$this->answer($mesh, $socket, substr($response, 0, 20), false, false);   // the head arrives in two reads
		self::assertSame(1, $mesh->getPendingCount(), 'An incomplete head is waited for.');
		$this->answer($mesh, $socket, substr($response, 20));

		self::assertSame(0, $mesh->getPendingCount());
		self::assertSame(1, $mesh->getPeerCount(), 'A verified 101 adds the link.');
		$mesh->connectPeer($uri);
		self::assertSame(0, $mesh->getPendingCount(), 'A linked URI is not dialed again.');
		$received = '';
		for ($i = 0; $i < 2000 && strlen($received) < 200; $i++) {
			$mesh->tick();
			$received .= (string) fread($socket, 8192);
			usleep(500);
		}
		$types = [];
		foreach ((new TWebSocketConnection(new TBufferStream(), false))->feed($received) as $message) {
			$envelope = TWebSocketEnvelope::decode($message);
			$types[$envelope->getType()] = $envelope->getPayload();
		}
		self::assertArrayHasKey(TWebSocketEnvelope::AUTH_CHALLENGE, $types, 'The dialer challenges the peer it dialed.');
		$answer = base64_encode(hash_hmac('sha256', "challenge:nonce-of-b\0A", sha1(self::SECRET), true));
		self::assertSame($answer, $types[TWebSocketEnvelope::AUTH_RESPONSE] ?? null, 'The challenge that came behind the 101 in the same read is answered.');
	}

	public function testAUriAnnouncedByALinkedNodeIsBoundToItsLinkAndNotDialed()
	{
		$mesh = $this->mesh('B', new DialRecordingMesh());
		$a = MeshTestPeer::dialedBy($mesh, 'A', self::SECRET, 'tcp://seed-a.example:9');
		$c = MeshTestPeer::acceptedBy($mesh, 'C', self::SECRET);
		$a->announce(['uri' => 'tcp://10.0.0.5:9']);   // A advertises its address, not the name it was dialed by
		$mesh->tick();
		self::assertSame([], $mesh->dialed, 'The address a linked node advertises is another name of its link.');
		$c->announce(['uri' => 'tcp://10.0.0.6:9'], 'A');   // an announce of A relayed over another link
		$mesh->tick();
		self::assertSame([], $mesh->dialed, 'Whichever link the announce arrives on.');
		$c->announce(['uri' => 'tcp://10.0.0.7:9'], 'D');
		$mesh->tick();
		self::assertSame(['tcp://10.0.0.7:9'], $mesh->dialed, 'The URI of a node that is not linked is dialed.');

		$a->transport->close();   // the link to A drops: every name of A is free again
		$mesh->tick();
		self::assertSame(1, $mesh->getPeerCount());
		foreach (['tcp://seed-a.example:9', 'tcp://10.0.0.5:9', 'tcp://10.0.0.6:9'] as $uri) {
			$c->announce(['uri' => $uri], 'A');
		}
		$mesh->tick();
		self::assertSame(['tcp://10.0.0.7:9', 'tcp://seed-a.example:9', 'tcp://10.0.0.5:9', 'tcp://10.0.0.6:9'], $mesh->dialed);
	}

	public function testOfTwoLinksDialedToOneNodeTheOlderRemains()
	{
		$mesh = $this->mesh('B', new DialRecordingMesh());
		$first = MeshTestPeer::dialedBy($mesh, 'A', self::SECRET, 'tcp://a-by-name.example:9');
		self::assertSame(1, $mesh->getPeerCount());
		self::assertContains(TWebSocketEnvelope::NODE_UP, $first->readTypes(), 'The link brings the node\'s full presence.');
		$second = MeshTestPeer::dialedBy($mesh, 'A', self::SECRET, 'tcp://10.0.0.5:9');   // a second seed that reaches the same node

		self::assertSame(1, $mesh->getPeerCount(), 'Two nodes keep one link.');
		self::assertSame([$first->meshTransport], $mesh->getSources(), 'The older link remains.');
		self::assertSame([], $first->readTypes(), 'A surplus link is closed as it is verified: it brings no full presence.');
		self::assertNotContains(TWebSocketEnvelope::NODE_UP, $second->readTypes(), 'And is sent none.');
		$c = MeshTestPeer::acceptedBy($mesh, 'C', self::SECRET);
		$c->announce(['uri' => 'tcp://10.0.0.5:9'], 'Z');
		$mesh->tick();
		self::assertSame([], $mesh->dialed, 'The URI of the closed link stays bound to the link that remains.');

		$first->transport->close();
		$mesh->tick();
		$c->announce(['uri' => 'tcp://10.0.0.5:9'], 'Y');
		$mesh->tick();
		self::assertSame(['tcp://10.0.0.5:9'], $mesh->dialed, 'It is free once no link reaches the node.');
		unset($second);
	}

	/**
	 * Links two real meshes over a socket pair; `$dialer` is the dialer.
	 * @param TMeshBackplane $dialer
	 * @param TMeshBackplane $acceptor
	 * @return array{0: TSocketStream, 1: TSocketStream} The dialer's and the acceptor's transports.
	 */
	private function link(TMeshBackplane $dialer, TMeshBackplane $acceptor): array
	{
		[$rawDialer, $rawAcceptor] = TSocketStream::pair();
		$rawDialer->setBlocking(false);
		$rawAcceptor->setBlocking(false);
		$dialer->addPeer(new TWebSocketConnection($rawDialer, true), $rawDialer);
		$acceptor->addPeer(new TWebSocketConnection($rawAcceptor, false), $rawAcceptor);
		return [$rawDialer, $rawAcceptor];
	}

	public function testOfTwoNodesThatDialEachOtherTheLinkOfTheLowerNodeIdRemains()
	{
		$a = $this->mesh('A');
		$b = $this->mesh('B');
		[$dialedByA, $acceptedByB] = $this->link($a, $b);
		$this->link($b, $a);   // the dials cross
		for ($i = 0; $i < 12; $i++) {
			$a->tick();
			$b->tick();
		}

		self::assertSame([$dialedByA], $a->getSources(), 'A, the lower node id, keeps the link it dialed.');
		self::assertSame([$acceptedByB], $b->getSources(), 'B closes the link it dialed and keeps the one A dialed.');

		$a->putPresence('A-1', ['node' => 'A']);
		$b->publish(new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'B', 'still linked'));
		for ($i = 0; $i < 4; $i++) {
			$a->tick();
			$b->tick();
		}
		self::assertSame(1, $a->getPeerCount());
		self::assertSame(1, $b->getPeerCount(), 'The remaining link stays up under traffic both ways.');
	}

	public function testOnlyTheDialerClosesASurplusLink()
	{
		// A 1.1.0 node keeps every link it dialed and dials a seed again when a link to it closes.  So a
		// node closes only links it dialed itself: an accepted surplus link is its dialer's to close.
		$low = $this->mesh('A');
		$first = MeshTestPeer::acceptedBy($low, 'Z', self::SECRET);
		$second = MeshTestPeer::acceptedBy($low, 'Z', self::SECRET);
		self::assertSame(2, $low->getPeerCount(), 'Two links a peer dialed are both kept: the peer closes the surplus one.');
		$third = MeshTestPeer::dialedBy($low, 'Z', self::SECRET, 'tcp://10.0.0.9:9');
		self::assertSame(3, $low->getPeerCount(), 'The lower node id keeps its own dial too, and the peer closes its own.');

		$high = $this->mesh('Z');
		$accepted = MeshTestPeer::acceptedBy($high, 'A', self::SECRET);
		$dialed = MeshTestPeer::dialedBy($high, 'A', self::SECRET, 'tcp://10.0.0.1:9');
		self::assertSame([$accepted->meshTransport], $high->getSources(), 'The higher node id closes its own dial when the peer has dialed it.');
		unset($first, $second, $third, $dialed);
	}
}
