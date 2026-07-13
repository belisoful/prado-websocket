<?php

/**
 * TMeshBackplane class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\IWebSocketEndpoint;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\Prado;
use Prado\TComponent;
use Prado\TPropertyValue;

/**
 * TMeshBackplane class.
 *
 * A backplane that relays cluster traffic over server-to-server WebSocket links, so nodes form a
 * mesh with no shared service or filesystem.  Each peer link is a {@see TWebSocketConnection} the
 * node dialed or accepted; an envelope is sent as one WebSocket message.
 *
 * Routing is epidemic, so the mesh need not be complete:
 *  - {@see publish()} floods the envelope to every peer; an inbound envelope is delivered to the
 *    local coordinator and re-flooded to the other peers.
 *  - A unique envelope {@see TWebSocketEnvelope::getId() id} is remembered, so a duplicate arriving
 *    over a second path is dropped and the flood terminates.
 *  - Channel interest needs no declaration (every node sees all traffic; the coordinator filters),
 *    so {@see subscribe()}/{@see unsubscribe()} are inert.
 *
 * Presence floods as presence envelopes, and a node sends its local presence to each peer as the
 * link is added, so a joining node converges.  The mesh self-assembles: it is seeded from the
 * {@see setPeers() Peers} dialed on {@see open()}, then each node announces its {@see setAdvertise()
 * Advertise} URI, which floods so other nodes dial it (a lexical tie-break makes exactly one side of
 * any pair initiate, and an established peer is never dialed twice).  Dials are asynchronous and
 * advance in {@see tick()}, so the serve loop never blocks on a connecting or handshaking peer.
 *
 * A peer joins only by proving the shared {@see setSecret() Secret} during its handshake
 * ({@see authenticate()}); the secret never crosses the wire, since the proof is an HMAC of the
 * connection's handshake key.  Set the secret and prefer a `tls://` transport on any untrusted
 * network; an unset secret leaves the mesh open to anyone who reaches the path.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 4.4.0
 */
class TMeshBackplane extends TComponent implements IWebSocketBackplane, IWebSocketEndpoint
{
	/** The maximum bytes read from a peer per drain. */
	public const READ_CHUNK = 65536;

	/** The number of envelope ids remembered for duplicate suppression. */
	public const SEEN_CAP = 4096;

	/** @var ?IWebSocketCluster The owning coordinator. */
	private ?IWebSocketCluster $_cluster = null;

	/** @var array<int, array{link: TWebSocketConnection, transport: TSocketStream, uri: string}> The peer links, keyed by link object id. */
	private array $_peers = [];

	/** @var array<int, array{transport: TSocketStream, uri: string, state: string, key: string, buffer: string, deadline: float}> In-flight async dials, keyed by transport object id. */
	private array $_pending = [];

	/** @var array<string, array<string, mixed>> The local clients' presence, for snapshotting to new peers. */
	private array $_localPresence = [];

	/** @var array<string, true> The recently seen envelope ids, for duplicate suppression. */
	private array $_seen = [];

	/** @var string[] The seed peer URIs dialed on open. */
	private array $_seedPeers = [];

	/** @var string The HTTP header carrying a peer's authentication proof. */
	public const AUTH_HEADER = 'X-Cluster-Auth';

	/** @var string The shared cluster secret; a peer proves it to join. '' leaves the mesh open (trusted networks only). */
	private string $_secret = '';

	/** @var string This node's own dialable URI, advertised so peers discover it; '' to stay undiscoverable. */
	private string $_advertise = '';

	/** @var array<string, true> The peer URIs already dialed or learned, so each is dialed at most once. */
	private array $_knownUris = [];

	/** @var array<string, true> The URIs a link is established to, so two nodes never keep a duplicate link. */
	private array $_connectedUris = [];

	/** @var string The Host header sent when dialing a peer. */
	private string $_host = 'cluster';

	/** @var string The request path used for a peer handshake. */
	private string $_path = '/cluster';

	/** @var float The dial timeout in seconds. */
	private float $_timeout = 2.0;

	/**
	 * Binds the owning coordinator.
	 * @param IWebSocketCluster $cluster The coordinator received envelopes are delivered to.
	 */
	public function setCluster(IWebSocketCluster $cluster): void
	{
		$this->_cluster = $cluster;
	}

	// =========================================================================
	// Lifecycle
	// =========================================================================

	/**
	 * Joins the mesh by dialing the seed peers.  A peer that cannot be reached is skipped, since the
	 * mesh tolerates a partial topology.
	 */
	public function open(): void
	{
		foreach ($this->_seedPeers as $uri) {
			try {
				$this->connectPeer($uri);
			} catch (\Throwable $e) {
				unset($this->_knownUris[$uri]);   // a seed that is down is reached later via gossip
			}
		}
	}

	/**
	 * Leaves the mesh, closing every peer link.
	 */
	public function close(): void
	{
		foreach ($this->_peers as $peer) {
			try {
				$peer['link']->close();
			} catch (\Throwable $e) {
				// A peer whose pipe is already broken needs no clean Close frame.
			}
		}
		foreach ($this->_pending as $pending) {
			$pending['transport']->close();
		}
		$this->_peers = [];
		$this->_pending = [];
		$this->_seen = [];
		$this->_knownUris = [];
		$this->_connectedUris = [];
	}

	/**
	 * Drains each peer link, delivering received envelopes and re-flooding them, and drops a peer
	 * whose link has closed.
	 */
	public function tick(): void
	{
		if ($this->_cluster === null) {
			return;
		}
		$this->advancePending();
		foreach ($this->_peers as $id => $peer) {
			$bytes = $this->readPeer($peer['transport']);
			if ($bytes === null || $peer['link']->getIsClosed()) {
				$this->dropPeer($id);
				continue;
			}
			if ($bytes !== '') {
				$this->ingest($id, $bytes);
			}
		}
	}

	/**
	 * Returns the peer and in-flight-dial transports to fold into the server's event loop.
	 * @return \Prado\IO\IResource[] The transports to select on.
	 */
	public function getSources(): array
	{
		return array_merge(array_column($this->_peers, 'transport'), array_column($this->_pending, 'transport'));
	}

	// =========================================================================
	// Routing
	// =========================================================================

	/**
	 * Floods an envelope to every peer.
	 * @param TWebSocketEnvelope $envelope The envelope to publish.
	 */
	public function publish(TWebSocketEnvelope $envelope): void
	{
		$this->markSeen($envelope->getId());
		$this->flood($envelope, null);
	}

	/**
	 * Does nothing; every node sees all flooded traffic, so channel interest needs no declaration.
	 * @param string $channel The channel (ignored).
	 */
	public function subscribe(string $channel): void
	{
	}

	/**
	 * Does nothing; every node sees all flooded traffic, so channel interest needs no declaration.
	 * @param string $channel The channel (ignored).
	 */
	public function unsubscribe(string $channel): void
	{
	}

	/**
	 * Records a local client and floods its presence to the mesh.
	 * @param string $clientId The cluster client id.
	 * @param array<string, mixed> $meta The presence metadata (already carries the node id).
	 */
	public function putPresence(string $clientId, array $meta): void
	{
		$this->_localPresence[$clientId] = $meta;
		$envelope = new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, $this->nodeId(), '', null, $clientId, $meta);
		$this->markSeen($envelope->getId());
		$this->flood($envelope, null);
	}

	/**
	 * Forgets a local client and floods its departure to the mesh.
	 * @param string $clientId The cluster client id.
	 */
	public function dropPresence(string $clientId): void
	{
		unset($this->_localPresence[$clientId]);
		$envelope = new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_DROP, $this->nodeId(), '', null, $clientId);
		$this->markSeen($envelope->getId());
		$this->flood($envelope, null);
	}

	// =========================================================================
	// Peers
	// =========================================================================

	/**
	 * Adds an established peer link and sends it the local presence snapshot so the peer converges.
	 * A known remote URI is recorded so the same peer is never linked twice.
	 * @param TWebSocketConnection $link The peer connection.
	 * @param TSocketStream $transport The connection's transport, for the event loop.
	 * @param string $uri The remote's URI when known (a dialed peer), or '' for an accepted peer.
	 */
	public function addPeer(TWebSocketConnection $link, TSocketStream $transport, string $uri = ''): void
	{
		$this->_peers[spl_object_id($link)] = ['link' => $link, 'transport' => $transport, 'uri' => $uri];
		if ($uri !== '') {
			$this->_connectedUris[$uri] = true;
			$this->_knownUris[$uri] = true;
		}
		foreach ($this->_localPresence as $clientId => $meta) {
			$this->sendTo($link, new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, $this->nodeId(), '', null, $clientId, $meta));
		}
		if ($this->_advertise !== '') {
			// Announce this node so the peer, and through its re-flood the rest of the mesh, can dial back.
			$this->sendTo($link, new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, $this->nodeId(), '', null, null, ['uri' => $this->_advertise]));
		}
	}

	/**
	 * Indicates whether an upgrade targets the mesh peer path, so the server hands it here as a peer
	 * link rather than a client.  Implements {@see IWebSocketEndpoint}.
	 * @param ?string $target The request target (path).
	 * @return bool Whether the target is the mesh {@see getPath() Path}.
	 */
	public function matchesTarget(?string $target): bool
	{
		return $this->_secret !== '' && $target !== null && $target === $this->_path;   // the endpoint is exposed only when a secret enables the mesh
	}

	/**
	 * Joins an authenticated, upgraded connection to the mesh as a peer.  Implements
	 * {@see IWebSocketEndpoint}.
	 * @param TWebSocketConnection $connection The upgraded peer connection.
	 * @param TSocketStream $transport The connection transport.
	 * @param array<string, mixed> $request The parsed upgrade request.
	 */
	public function accept(TWebSocketConnection $connection, TSocketStream $transport, array $request): void
	{
		$this->addPeer($connection, $transport);
	}

	/**
	 * Begins dialing a peer without blocking: it opens an asynchronous connection and registers the
	 * attempt, which {@see tick()} advances through the connect and WebSocket handshake before adding
	 * the link.  A blocking dial would stall the serve loop, so discovery and reconnection stay async.
	 * @param string $uri The peer endpoint, e.g. 'tcp://10.0.0.2:8080'.
	 */
	public function connectPeer(string $uri): void
	{
		if ($this->_secret === '' || isset($this->_connectedUris[$uri])) {
			return;   // the mesh is disabled without a secret, or already linked to this peer
		}
		if (!preg_match('#^(tcp|tls|ssl)://#i', $uri)) {
			return;   // only dial stream sockets; refuse gossiped non-socket URIs (SSRF hardening)
		}
		$this->_knownUris[$uri] = true;
		$transport = TSocketStream::connect($uri, $this->_timeout, STREAM_CLIENT_ASYNC_CONNECT | STREAM_CLIENT_CONNECT);
		$transport->setBlocking(false);
		$this->_pending[spl_object_id($transport)] = [
			'transport' => $transport,
			'uri' => $uri,
			'state' => 'connecting',
			'key' => '',
			'buffer' => '',
			'deadline' => $this->now() + $this->_timeout,
		];
	}

	/**
	 * Verifies an inbound peer handshake against the shared secret.  The proof is an HMAC of the
	 * connection's `Sec-WebSocket-Key` keyed by the secret, so the secret never crosses the wire and
	 * a captured proof cannot authenticate a different connection (each handshake key is fresh).  The
	 * mesh is disabled without a secret, so an unauthenticated peer can never join.
	 * @param array<string, string> $headers The lower-cased request headers.
	 * @return bool Whether the peer is authorized to join.
	 */
	public function authenticate(array $headers): bool
	{
		if ($this->_secret === '') {
			return false;   // the mesh requires a shared secret; an empty one leaves it disabled, never open
		}
		$key = $headers['sec-websocket-key'] ?? '';
		$provided = $headers[strtolower(self::AUTH_HEADER)] ?? '';
		return $key !== '' && $provided !== '' && hash_equals($this->authToken($key), $provided);
	}

	/**
	 * Computes the authentication proof for a handshake key.
	 * @param string $key The connection's `Sec-WebSocket-Key`.
	 * @return string The base64 HMAC-SHA256 of the key under the shared secret.
	 */
	private function authToken(string $key): string
	{
		return base64_encode(hash_hmac('sha256', $key, sha1($this->_secret), true));
	}

	/**
	 * Returns the number of live peer links.
	 * @return int The peer count.
	 */
	public function getPeerCount(): int
	{
		return count($this->_peers);
	}

	/**
	 * Returns the number of in-flight dials not yet established.
	 * @return int The pending dial count.
	 */
	public function getPendingCount(): int
	{
		return count($this->_pending);
	}

	// =========================================================================
	// Internals
	// =========================================================================

	/**
	 * Delivers a received envelope to the coordinator and re-floods it to the other peers, dropping
	 * a duplicate already seen.
	 * @param TWebSocketEnvelope $envelope The received envelope.
	 * @param int $fromPeerId The id of the peer it arrived on.
	 */
	private function receive(TWebSocketEnvelope $envelope, int $fromPeerId): void
	{
		if (isset($this->_seen[$envelope->getId()])) {
			return;
		}
		$this->markSeen($envelope->getId());
		$this->_cluster?->receiveEnvelope($envelope);
		$this->flood($envelope, $fromPeerId);
		if ($envelope->getType() === TWebSocketEnvelope::NODE_UP) {
			$this->discover($fromPeerId, (string) ($envelope->getMeta()['uri'] ?? ''));
		}
	}

	/**
	 * Acts on a node announce: the first announce on a link is the directly-connected peer naming
	 * itself, so its URI is bound to the link (never dialed again); a later announce names a distant
	 * node to learn and dial.
	 * @param int $fromPeerId The peer the announce arrived on.
	 * @param string $uri The advertised URI.
	 */
	private function discover(int $fromPeerId, string $uri): void
	{
		if ($uri === '') {
			return;
		}
		if (isset($this->_peers[$fromPeerId]) && $this->_peers[$fromPeerId]['uri'] === '') {
			$this->_peers[$fromPeerId]['uri'] = $uri;
			$this->_connectedUris[$uri] = true;
			$this->_knownUris[$uri] = true;
			return;
		}
		$this->learnPeer($uri);
	}

	/**
	 * Learns a peer URI from a gossiped announce and dials it once.  A node dials only the URIs that
	 * sort after its own {@see getAdvertise() Advertise} (or all, when it advertises nothing), so of
	 * any two nodes exactly one initiates the link.
	 * @param string $uri The advertised peer URI.
	 */
	private function learnPeer(string $uri): void
	{
		if ($uri === '' || $uri === $this->_advertise || isset($this->_knownUris[$uri]) || isset($this->_connectedUris[$uri])) {
			return;
		}
		$this->_knownUris[$uri] = true;
		if ($this->_advertise !== '' && strcmp($uri, $this->_advertise) <= 0) {
			return;   // a lower-sorting peer dials this node, not the reverse
		}
		try {
			$this->connectPeer($uri);
		} catch (\Throwable $e) {
			unset($this->_knownUris[$uri]);   // unreachable now; retry when its announce is gossiped again
		}
	}

	/**
	 * Sends an envelope to every peer except an optional source.
	 * @param TWebSocketEnvelope $envelope The envelope to send.
	 * @param ?int $exceptPeerId The peer id to skip (the source of a re-flood), or null.
	 */
	private function flood(TWebSocketEnvelope $envelope, ?int $exceptPeerId): void
	{
		foreach ($this->_peers as $id => $peer) {
			if ($id !== $exceptPeerId) {
				$this->sendTo($peer['link'], $envelope);
			}
		}
	}

	/**
	 * Sends an envelope over a peer link, dropping the peer when the send fails.
	 * @param TWebSocketConnection $link The peer connection.
	 * @param TWebSocketEnvelope $envelope The envelope to send.
	 */
	private function sendTo(TWebSocketConnection $link, TWebSocketEnvelope $envelope): void
	{
		try {
			if (!$link->getIsClosed()) {
				$link->send($envelope->encode());
			}
		} catch (\Throwable $e) {
			$this->dropPeer(spl_object_id($link));
		}
	}

	/**
	 * Reads a chunk from a peer transport, treating end of stream and a read failure alike.
	 * @param TSocketStream $transport The peer transport.
	 * @return ?string The bytes read, or null at end of stream or on a read failure.
	 */
	private function readPeer(TSocketStream $transport): ?string
	{
		try {
			$bytes = $transport->read(self::READ_CHUNK);
		} catch (\RuntimeException $e) {
			return null;
		}
		return ($bytes === '' && $transport->eof()) ? null : $bytes;
	}

	/**
	 * Closes and forgets a peer link.
	 * @param int $id The peer object id.
	 */
	private function dropPeer(int $id): void
	{
		if (!isset($this->_peers[$id])) {
			return;
		}
		$uri = $this->_peers[$id]['uri'];
		try {
			$this->_peers[$id]['link']->close();
		} catch (\Throwable $e) {
			// The peer is already gone; a clean Close frame cannot be sent.
		}
		unset($this->_peers[$id]);
		if ($uri !== '') {
			unset($this->_connectedUris[$uri], $this->_knownUris[$uri]);   // allow re-discovery and re-dial
		}
	}

	/**
	 * Feeds bytes from a peer into the codec and routes any complete envelopes.
	 * @param int $peerId The peer the bytes arrived on.
	 * @param string $bytes The bytes read.
	 */
	private function ingest(int $peerId, string $bytes): void
	{
		if (!isset($this->_peers[$peerId])) {
			return;
		}
		try {
			$messages = $this->_peers[$peerId]['link']->feed($bytes);
		} catch (\Throwable $e) {
			// A protocol error, or an I/O failure echoing a Close to a vanished peer, drops it.
			$this->dropPeer($peerId);
			return;
		}
		foreach ($messages as $message) {
			$envelope = TWebSocketEnvelope::decode($message);
			if ($envelope !== null) {
				$this->receive($envelope, $peerId);
			}
		}
	}

	/**
	 * Advances each in-flight dial: a connected socket sends the handshake, a handshaking one reads
	 * the response, and a timed-out or failed attempt is dropped.
	 */
	private function advancePending(): void
	{
		foreach ($this->_pending as $id => $pending) {
			$resource = $pending['transport']->getResource();
			if (!is_resource($resource) || $this->now() > $pending['deadline']) {
				$this->failPending($id);
				continue;
			}
			if ($pending['state'] === 'connecting') {
				if ($this->ready($resource, true)) {
					$this->sendPeerHandshake($id);
				}
			} elseif ($this->ready($resource, false)) {
				$this->readPeerHandshake($id);
			}
		}
	}

	/**
	 * Reports whether a resource is ready for reading or writing, without blocking.
	 * @param resource $resource The socket resource.
	 * @param bool $forWrite Whether to test writability rather than readability.
	 * @return bool Whether the resource is ready.
	 */
	private function ready($resource, bool $forWrite): bool
	{
		$read = $forWrite ? null : [$resource];
		$write = $forWrite ? [$resource] : null;
		$except = null;
		return @stream_select($read, $write, $except, 0) > 0;
	}

	/**
	 * Sends the WebSocket upgrade request once a dialed socket has connected.
	 * @param int $id The pending dial id.
	 */
	private function sendPeerHandshake(int $id): void
	{
		$transport = $this->_pending[$id]['transport'];
		if (@stream_socket_get_name($transport->getResource(), true) === false) {
			$this->failPending($id);   // the connection attempt did not succeed
			return;
		}
		$key = TWebSocketHandshake::generateKey();
		$headers = $this->_secret === '' ? [] : [self::AUTH_HEADER => $this->authToken($key)];
		try {
			$transport->write(TWebSocketHandshake::buildClientRequest($this->_host, $this->_path, $key, $headers));
		} catch (\Throwable $e) {
			$this->failPending($id);
			return;
		}
		$this->_pending[$id]['key'] = $key;
		$this->_pending[$id]['state'] = 'handshaking';
	}

	/**
	 * Reads the server's handshake response for a dialed socket and, on a verified 101, adds the peer.
	 * @param int $id The pending dial id.
	 */
	private function readPeerHandshake(int $id): void
	{
		$pending = $this->_pending[$id];
		$transport = $pending['transport'];
		try {
			$bytes = $transport->read(self::READ_CHUNK);
		} catch (\RuntimeException $e) {
			$this->failPending($id);
			return;
		}
		if ($bytes === '' && $transport->eof()) {
			$this->failPending($id);
			return;
		}
		$buffer = $pending['buffer'] . $bytes;
		if (!str_contains($buffer, "\r\n\r\n")) {
			if (strlen($buffer) > self::READ_CHUNK) {
				$this->failPending($id);
				return;
			}
			$this->_pending[$id]['buffer'] = $buffer;
			return;
		}
		$response = TWebSocketHandshake::parseHttpMessage($buffer);
		if (!TWebSocketHandshake::verifyServerResponse($response, $pending['key'])) {
			$this->failPending($id);   // rejected (e.g. 403) or not a valid 101
			return;
		}
		unset($this->_pending[$id]);
		$link = Prado::createComponent(TWebSocketConnection::class, $transport, true);
		$this->addPeer($link, $transport, $pending['uri']);
		if ($response['body'] !== '') {
			$this->ingest(spl_object_id($link), $response['body']);   // frames sent right after the 101
		}
	}

	/**
	 * Closes and forgets an in-flight dial, allowing the URI to be dialed again later.
	 * @param int $id The pending dial id.
	 */
	private function failPending(int $id): void
	{
		if (!isset($this->_pending[$id])) {
			return;
		}
		$uri = $this->_pending[$id]['uri'];
		$this->_pending[$id]['transport']->close();
		unset($this->_pending[$id]);
		unset($this->_knownUris[$uri]);
	}

	/**
	 * Returns the current time in seconds, isolated for testing.
	 * @return float The current time.
	 */
	protected function now(): float
	{
		return microtime(true);
	}

	/**
	 * Records an envelope id as seen, trimming the oldest half when the cap is exceeded.
	 * @param string $id The envelope id.
	 */
	private function markSeen(string $id): void
	{
		$this->_seen[$id] = true;
		if (count($this->_seen) > self::SEEN_CAP) {
			$this->_seen = array_slice($this->_seen, intdiv(self::SEEN_CAP, 2), null, true);
		}
	}

	/**
	 * Returns the local node id.
	 * @return string The node id, or '' when no coordinator is bound.
	 */
	private function nodeId(): string
	{
		return $this->_cluster !== null ? $this->_cluster->getNodeId() : '';
	}

	// =========================================================================
	// Properties
	// =========================================================================

	/**
	 * Returns the seed peer URIs.
	 * @return string[] The seed peer URIs.
	 */
	public function getPeers(): array
	{
		return $this->_seedPeers;
	}

	/**
	 * Sets the seed peer URIs dialed on open.
	 * @param array|string $value The peer URIs, as an array or a comma-delimited string.
	 * @return static The current backplane.
	 */
	public function setPeers($value): static
	{
		$this->_seedPeers = TPropertyValue::ensureArray($value);
		return $this;
	}

	/**
	 * Returns the shared cluster secret.
	 * @return string The secret, or '' when the mesh is open.
	 */
	public function getSecret(): string
	{
		return $this->_secret;
	}

	/**
	 * Sets the shared cluster secret a peer must prove to join.  Configure it (and prefer a `tls://`
	 * transport) on any untrusted network; an empty secret accepts any peer that reaches the path.
	 * @param string $value The secret.
	 * @return static The current backplane.
	 */
	public function setSecret($value): static
	{
		$this->_secret = TPropertyValue::ensureString($value);
		return $this;
	}

	/**
	 * Returns this node's advertised URI.
	 * @return string The advertised URI, or '' when undiscoverable.
	 */
	public function getAdvertise(): string
	{
		return $this->_advertise;
	}

	/**
	 * Sets this node's own dialable URI, announced to peers so the mesh discovers it.  Leave empty to
	 * stay undiscoverable (the node still dials out and accepts inbound peers).
	 * @param string $value The advertised URI, e.g. 'tcp://10.0.0.1:8080'.
	 * @return static The current backplane.
	 */
	public function setAdvertise($value): static
	{
		$this->_advertise = TPropertyValue::ensureString($value);
		return $this;
	}

	/**
	 * Returns the Host header sent when dialing a peer.
	 * @return string The host.
	 */
	public function getHost(): string
	{
		return $this->_host;
	}

	/**
	 * Sets the Host header sent when dialing a peer.
	 * @param string $value The host.
	 * @return static The current backplane.
	 */
	public function setHost($value): static
	{
		$this->_host = TPropertyValue::ensureString($value);
		return $this;
	}

	/**
	 * Returns the request path used for a peer handshake.
	 * @return string The path.
	 */
	public function getPath(): string
	{
		return $this->_path;
	}

	/**
	 * Sets the request path used for a peer handshake.
	 * @param string $value The path.
	 * @return static The current backplane.
	 */
	public function setPath($value): static
	{
		$this->_path = TPropertyValue::ensureString($value);
		return $this;
	}

	/**
	 * Returns the dial timeout in seconds.
	 * @return float The timeout.
	 */
	public function getTimeout(): float
	{
		return $this->_timeout;
	}

	/**
	 * Sets the dial timeout in seconds.
	 * @param float|string $value The timeout.
	 * @return static The current backplane.
	 */
	public function setTimeout($value): static
	{
		$this->_timeout = TPropertyValue::ensureFloat($value);
		return $this;
	}
}
