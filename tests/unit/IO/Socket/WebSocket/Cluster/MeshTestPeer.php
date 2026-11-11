<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;

/**
 * The far end of a mesh link, driven by hand.  It proves the shared secret to a real
 * {@see TMeshBackplane} as a node does, then sends the envelopes a test gives it and collects what
 * the backplane sends, so a test decides exactly what a peer says, including what a 1.1.0 node says.
 */
class MeshTestPeer
{
	/** @var TWebSocketConnection The peer's end of the link. */
	public TWebSocketConnection $link;

	/** @var TSocketStream The peer's transport. */
	public TSocketStream $transport;

	/** @var TSocketStream The backplane's transport for the link, as its sources list it. */
	public TSocketStream $meshTransport;

	/** @var string The node id the peer proved. */
	public string $node;

	private function __construct(TWebSocketConnection $link, TSocketStream $transport, TSocketStream $meshTransport, string $node)
	{
		$this->link = $link;
		$this->transport = $transport;
		$this->meshTransport = $meshTransport;
		$this->node = $node;
	}

	/**
	 * Links a peer the backplane accepts: the peer is the dialer.
	 * @param TMeshBackplane $mesh The backplane under test.
	 * @param string $node The node id the peer proves.
	 * @param string $secret The shared secret.
	 * @return self The verified peer.
	 */
	public static function acceptedBy(TMeshBackplane $mesh, string $node, string $secret): self
	{
		return self::link($mesh, $node, $secret, false, '');
	}

	/**
	 * Links a peer the backplane dialed at a URI: the peer is the acceptor.
	 * @param TMeshBackplane $mesh The backplane under test.
	 * @param string $node The node id the peer proves.
	 * @param string $secret The shared secret.
	 * @param string $uri The URI the backplane dialed.
	 * @return self The verified peer.
	 */
	public static function dialedBy(TMeshBackplane $mesh, string $node, string $secret, string $uri): self
	{
		return self::link($mesh, $node, $secret, true, $uri);
	}

	private static function link(TMeshBackplane $mesh, string $node, string $secret, bool $meshDials, string $uri): self
	{
		[$rawPeer, $rawMesh] = TSocketStream::pair();
		$rawPeer->setBlocking(false);
		$rawMesh->setBlocking(false);
		$mesh->addPeer(new TWebSocketConnection($rawMesh, $meshDials), $rawMesh, $uri);
		$peer = new self(new TWebSocketConnection($rawPeer, !$meshDials), $rawPeer, $rawMesh, $node);
		$nonce = '';
		foreach ($peer->read() as $envelope) {
			if ($envelope->getType() === TWebSocketEnvelope::AUTH_CHALLENGE) {
				$nonce = $envelope->getPayload();
			}
		}
		$answer = base64_encode(hash_hmac('sha256', 'challenge:' . $nonce . "\0" . $node, sha1($secret), true));
		$peer->send(new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_RESPONSE, $node, $answer));
		$mesh->tick();
		return $peer;
	}

	/**
	 * Sends an envelope to the backplane.
	 * @param TWebSocketEnvelope $envelope The envelope.
	 */
	public function send(TWebSocketEnvelope $envelope): void
	{
		$this->link->send($envelope->encode());
	}

	/**
	 * Sends a node announce as the peer's node, or as another node the peer relays.
	 * @param array<string, mixed> $meta The announce metadata.
	 * @param ?string $origin The announcing node; the peer's own by default.
	 */
	public function announce(array $meta = [], ?string $origin = null): void
	{
		$this->send(new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, $origin ?? $this->node, '', null, null, $meta + ['uri' => '']));
	}

	/**
	 * Reads the envelopes the backplane has sent since the last read.
	 * @return TWebSocketEnvelope[] The envelopes, in order.
	 */
	public function read(): array
	{
		$envelopes = [];
		while (($bytes = $this->transport->read(1 << 20)) !== '') {
			foreach ($this->link->feed($bytes) as $message) {
				$envelope = TWebSocketEnvelope::decode($message);
				if ($envelope !== null) {
					$envelopes[] = $envelope;
				}
			}
		}
		return $envelopes;
	}

	/**
	 * Reads the types of the envelopes the backplane has sent since the last read.
	 * @return string[] The envelope types, in order.
	 */
	public function readTypes(): array
	{
		return array_map(fn (TWebSocketEnvelope $envelope) => $envelope->getType(), $this->read());
	}
}
