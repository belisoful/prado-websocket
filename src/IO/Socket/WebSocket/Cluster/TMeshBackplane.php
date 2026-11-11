<?php

/**
 * TMeshBackplane class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

use Prado\Exceptions\TConfigurationException;
use Prado\Exceptions\TInvalidDataValueException;
use Prado\IO\IResource;
use Prado\IO\Process\TProcess;
use Prado\IO\Socket\TSocketAddress;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\IWebSocketEndpointLimits;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\Prado;
use Prado\TComponent;
use Prado\TPropertyValue;
use Prado\Util\Log\TLogger;
use Prado\Util\Clock\TApplicationClockAwareTrait;
use Prado\Util\Clock\TMonotonicClockAwareTrait;

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
 * Presence floods as presence envelopes.  When a link is verified, each end floods its full presence
 * to every peer ({@see announcePresence()}): an opening announce, one presence envelope per local
 * client, and a closing announce.  Each full presence has a generation number.  The announces carry
 * it in their metadata, with `presence` set to `begin` or `end`, and every presence envelope of the
 * node carries it as its payload.  A node that receives the closing announce drops the clients it
 * holds for the sender that no envelope of that generation set.  So the full presence replaces what
 * the node held, and a client that left while a link was down is forgotten.  A client that is still
 * present is never removed in between.  A presence envelope of an older generation than the newest
 * heard from its node is ignored.
 *
 * Every node announce also carries the sender's incarnation, a value drawn once per backplane
 * instance.  A node that hears a new incarnation for a node id drops the clients it holds for that id
 * before applying the new incarnation's presence, so a node that restarts under a static node id
 * leaves no phantom clients.  A 1.1.0 node ignores the generation and the `presence` mark: it adds
 * what a full presence carries and removes nothing, as it does among 1.1.0 nodes.
 *
 * The mesh self-assembles: it is seeded from the {@see setPeers() Peers} dialed on {@see open()},
 * then each node announces its {@see setAdvertise() Advertise} URI, which floods so other nodes dial
 * it (a lexical tie-break makes exactly one side of any pair initiate, and an established peer is
 * never dialed twice).  A {@see setPeerPattern() PeerPattern} limits which announced URIs a node
 * dials; seeds are always dialed.
 *
 * Two nodes keep one link.  A URI announced by a node this node is linked to is recorded as another
 * name of that link and is not dialed, so a seed written with a hostname and advertised by its address
 * is linked once.  When two verified links reach the same node, the node that dialed the surplus link
 * closes it.  Of two links it dialed itself it keeps the older.  Of two links the nodes dialed at each
 * other, the one dialed by the node with the lower node id remains.
 *
 * Dials do not block the serve loop.  The TCP connect is asynchronous; a `tls://` or `ssl://` peer is
 * connected over TCP first and its TLS handshake is then driven without blocking, with the
 * {@see setSslContext() SslContext} options and the URI's host as the peer name; the WebSocket
 * handshake follows.  A connecting dial is one of the {@see getWriteSources() write sources}, so the
 * server's writability check advances it, and a handshaking one advances when it is readable.  Every
 * step runs under the {@see setTimeout() Timeout}.
 *
 * A hostname resolves without blocking the loop either.  PHP has no non-blocking resolver, so a
 * short child process ({@see startResolver()}) runs the system resolver (`/etc/hosts`, the search
 * domains, a container's service names) and writes the addresses to its stdout, which is one of
 * the {@see getSources() sources}; the dial then connects to the first address, keeping the
 * hostname as the TLS peer name.  The child returns the host's IPv4 addresses.  For a host without
 * one it returns the address the system resolver picks for a datagram socket to the host, which
 * covers an IPv6-only name in `/etc/hosts` or under a search domain, and then the host's `AAAA`
 * records.  The lookup runs under the dial {@see setTimeout() Timeout}, one per host at a time and at
 * most {@see MAX_RESOLVERS} at once, the rest waiting their turn.  When 256 hosts are waiting, a dial
 * to a further host is put off until its URI is announced or re-dialed again.  A lookup's result is
 * cached for {@see RESOLVE_TTL} seconds; a dial that fails drops the cached address, so a peer that
 * moved is looked up again.
 *
 * A peer URI's host is one of:
 *  - a DNS name, whose labels may contain underscores, as container and service names do;
 *  - an IPv4 or IPv6 address, dialed without a lookup;
 *  - an IPv6 address with a zone, such as `[fe80::1%eth0]`, dialed as written without a lookup.
 *
 * The child is the PHP CLI binary {@see getResolverBinary() ResolverBinary}: by default the running
 * binary under the `cli` and `cli-server` SAPIs, else `php` in PHP's binary directory.  Under another
 * SAPI, such as php-fpm, with no CLI binary there, a hostname cannot be looked up:
 * {@see connectPeer()} refuses the dial and a warning is logged once.  Set ResolverBinary to a PHP
 * CLI binary in that case.  Peers given by IP address are unaffected.
 *
 * A lookup past its timeout, and every lookup still running at {@see close()}, is killed, and
 * close() waits up to {@see RESOLVER_REAP_TIME} seconds for the children to exit.  A child inherits
 * the open sockets of this process, the listener included, since PHP cannot start a process without
 * them.  It holds them until it exits, which the lookup's timeout and close() bound.
 *
 * A peer joins only by proving the shared {@see setSecret() Secret}: first an HMAC of its handshake
 * key ({@see authenticate()}), then a *mutual* fresh-nonce challenge after the upgrade
 * ({@see addPeer()}) in which each side signs the other's nonce together with its own node id and
 * shows no state until it has verified the peer.  The dialing side answers first: the accepting side
 * holds the dialer's challenge until it has verified the dialer's answer.  So an accepted link can
 * never obtain a signature, and a peer that replays a captured handshake to two nodes cannot use one
 * as a signing oracle to pass the other's challenge.  A challenge cannot be reflected back at its
 * issuer, and a node a peer dials cannot harvest that node's presence or advertised URI without the
 * secret.  The secret never crosses the wire.  Only an endpoint this node chose to dial (a seed, or
 * a URI announced by a verified peer) obtains its answer, so use a `tls://` transport on any
 * untrusted network, where an address can be taken over.  The secret is required: {@see open()}
 * refuses to start without one, since no peer could ever join.
 *
 * A peer link buffers output the kernel does not take at once.  {@see tick()} flushes it, and the
 * server watches each link with output pending for writability ({@see IWebSocketBufferedBackplane}),
 * so a message larger than the socket buffer drains as fast as the peer reads it.
 *
 * Liveness is tracked per node from the traffic it originates.  A node unheard for the
 * {@see getNodeTtl() TTL} is declared down: its clients leave the presence mirror and any direct link
 * to it is dropped, which frees its URI so it is dialed again when it returns.  Seed peers that are
 * not linked are re-dialed once per TTL.
 *
 * Presence converges when a link returns and when a node is heard again.  A link drops on a reset,
 * on a full send buffer, or on a malformed frame, and the presence changes flooded while it was down
 * did not cross it.  When a link is verified, each end floods its full presence and sends the peer a
 * {@see TWebSocketEnvelope::NODE_SYNC} naming the nodes it has heard and holds no link to; the peer
 * relays the request to the nodes behind it.  A node that receives traffic from a node it holds no
 * liveness for (one it never heard, or one it declared down) floods such a request naming it, once
 * per tick for all such nodes, unless what it heard opens that node's full presence.  A named node
 * floods its full presence, at most once per {@see ANNOUNCE_INTERVAL}.  So the nodes on each side of
 * a restored link replace what they hold for the nodes on the other side.  A 1.1.0 node relays the
 * request and does not answer it.
 *
 * An accepted peer that has not proven the secret may send messages of at most
 * {@see UNVERIFIED_MESSAGE_SIZE} bytes, enough for the challenge exchange, so a link held open with a
 * replayed handshake cannot buffer a full
 * {@see \Prado\IO\Socket\WebSocket\TWebSocketConnection::getMaxMessageSize() MaxMessageSize}.
 *
 * Both ends of a link apply {@see getLinkMaxMessageSize() LinkMaxMessageSize} and
 * {@see getLinkMaxSendBufferBytes() LinkMaxSendBufferBytes}, not the server's client limits, so an
 * envelope that carries a client message near the client limit, larger once base64-encoded, does
 * not drop the link.
 *
 * Durations within the process (the heartbeat, scan and re-dial intervals, the dial and challenge
 * deadlines, and the liveness of each node, from local receipt times) are measured on the
 * {@see getMonotonicClock() MonotonicClock}, so a wall-clock step neither expires nor extends them.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class TMeshBackplane extends TComponent implements IWebSocketBufferedBackplane, IWebSocketEndpointLimits
{
	use TApplicationClockAwareTrait;
	use TMonotonicClockAwareTrait;

	/** The maximum bytes read from a peer per drain. */
	public const READ_CHUNK = 65536;

	/** The number of envelope ids remembered for duplicate suppression; a busy mesh cycles through many ids within one TTL. */
	public const SEEN_CAP = 65536;

	/**
	 * The largest message, in bytes, an accepted peer may send before it proves the secret: room for
	 * the challenge and its answer.
	 * @since 1.2.0
	 */
	public const UNVERIFIED_MESSAGE_SIZE = 4096;

	/**
	 * The fewest seconds between two re-announces of this node's presence that {@see TWebSocketEnvelope::NODE_SYNC}
	 * requests trigger; requests in between are answered by one re-announce.
	 * @since 1.2.0
	 */
	public const ANNOUNCE_INTERVAL = 1.0;

	/**
	 * The seconds a hostname's resolved addresses are reused before it is looked up again.
	 * @since 1.2.0
	 */
	public const RESOLVE_TTL = 60.0;

	/**
	 * The most hostname lookups that run at once, each in its own child process; further lookups wait
	 * their turn.
	 * @since 1.2.0
	 */
	public const MAX_RESOLVERS = 8;

	/**
	 * The seconds {@see close()} waits for the resolver children it killed to exit.
	 * @since 1.2.0
	 */
	public const RESOLVER_REAP_TIME = 1.0;

	/** The most hostnames that wait for a lookup; a dial to a further host is put off until its URI is dialed again. */
	private const RESOLVE_QUEUE_CAP = 256;

	/**
	 * The resolver child's program.  It writes a JSON list: the system resolver's IPv4 addresses, else
	 * the address the system resolver connects a datagram socket to (which sends nothing), else the
	 * host's `AAAA` records.
	 */
	private const RESOLVER_CODE = '$h = $argv[1] ?? \'\'; $a = @gethostbynamel($h) ?: [];'
		. ' if ($a === [] && ($s = @stream_socket_client(\'udp://\' . $h . \':9\', $e, $m, 1)) && is_string($n = stream_socket_get_name($s, true))) { $a[] = trim(substr($n, 0, (int) strrpos($n, \':\')), \'[]\'); }'
		. ' if ($a === [] && function_exists(\'dns_get_record\')) { foreach (@dns_get_record($h, DNS_AAAA) ?: [] as $r) { if (isset($r[\'ipv6\'])) { $a[] = $r[\'ipv6\']; } } }'
		. ' echo json_encode(array_values(array_unique($a)));';

	/** The pattern of a hostname the resolver is asked for: DNS labels, which may contain underscores, up to 253 characters. */
	private const HOST_PATTERN = '/^(?=.{1,253}\.?$)[A-Za-z0-9_](?:[A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?(?:\.[A-Za-z0-9_](?:[A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?)*\.?$/D';

	/** The pattern of an IPv6 address with a zone: the address, `%`, and an interface name or index. */
	private const SCOPED_ADDRESS_PATTERN = '/^([0-9A-Fa-f:.]+)%[A-Za-z0-9._-]{1,64}$/D';

	/** The `presence` mark of the announce that opens a node's full presence. */
	private const PRESENCE_BEGIN = 'begin';

	/** The `presence` mark of the announce that closes a node's full presence. */
	private const PRESENCE_END = 'end';

	/** The signal that ends a resolver child.  It cannot be caught, so it never runs a handler in a child that has not yet started its program. */
	private const SIGKILL = 9;

	/** @var ?IWebSocketCluster The owning coordinator. */
	private ?IWebSocketCluster $_cluster = null;

	/** @var array<int, array{link: TWebSocketConnection, transport: TSocketStream, uri: string, aliases: array<string, true>, verified: bool, nonce: string, heldNonce: string, authDeadline: float, nodeId: string, limit: int}> The peer links, keyed by link object id; `aliases` are the further URIs known to reach the same node; `nodeId` is the peer's node id once it has proven the secret, '' before; `heldNonce` is an accepted peer's challenge, answered once it is verified; `limit` is the link's message size limit to restore once it is verified. */
	private array $_peers = [];

	/** @var array<int, array{transport: TSocketStream, uri: string, state: string, secure: bool, key: string, buffer: string, deadline: float}> In-flight async dials, keyed by transport object id; `state` is 'connecting', 'securing' (the TLS handshake), or 'handshaking' (the WebSocket upgrade). */
	private array $_pending = [];

	/** @var array<string, array{process: ?TProcess, buffer: string, deadline: float, uris: array<string, true>}> The hostname lookups not yet answered, keyed by host in the order asked; `process` is null while the lookup waits for a turn under {@see MAX_RESOLVERS}; `uris` are the dials waiting on it. */
	private array $_resolving = [];

	/** @var array<string, array{address: string, expires: float}> Resolved hostnames, keyed by host. */
	private array $_resolved = [];

	/** @var TProcess[] Resolver children killed at their deadline or at {@see close()}, closed once they exit. */
	private array $_reaping = [];

	/** @var ?string The PHP CLI binary that runs a hostname lookup; null until first used, '' when none is available. */
	private ?string $_resolverBinary = null;

	/** @var bool Whether the warning about an unusable resolver binary has been logged. */
	private bool $_resolverWarned = false;

	/** @var array<string, array<string, mixed>> The local clients' presence, for snapshotting to new peers. */
	private array $_localPresence = [];

	/** @var array<string, true> The recently seen envelope ids, for duplicate suppression. */
	private array $_seen = [];

	/** @var string[] The seed peer URIs dialed on open. */
	private array $_seedPeers = [];

	/** @var string The HTTP header carrying a peer's authentication proof. */
	public const AUTH_HEADER = 'X-Cluster-Auth';

	/** The domain-separation tag for the handshake proof, so a proof cannot be reused as a challenge answer. */
	private const AUTH_CONTEXT_HANDSHAKE = 'handshake';

	/** The domain-separation tag for the challenge answer, so an answer cannot be reused as a handshake proof. */
	private const AUTH_CONTEXT_CHALLENGE = 'challenge';

	/** The separator between a nonce and the answering node's id inside a challenge answer. */
	private const AUTH_ANSWER_SEPARATOR = "\0";

	/** @var string The shared cluster secret a peer proves to join; required, '' disables the mesh. */
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

	/** @var int The maximum peers accepted but not yet verified, or 0 for unlimited; bounds a connection flood from a replayed handshake proof. */
	private int $_maxPendingPeers = 64;

	/** @var array<string, float> The {@see \Prado\Util\Clock\TMonotonicClock::seconds()} an envelope was last received from each node, for liveness. */
	private array $_nodeSeen = [];

	/** @var int The seconds a node may go unheard before it is declared down and its presence reaped. */
	private int $_nodeTtl = 30;

	/** @var ?float The {@see \Prado\Util\Clock\TMonotonicClock::seconds()} a heartbeat was last flooded, or null before the first. */
	private ?float $_lastHeartbeat = null;

	/** @var ?float The {@see \Prado\Util\Clock\TMonotonicClock::seconds()} the failure detector last scanned, or null before the first. */
	private ?float $_lastNodeScan = null;

	/** @var ?float The {@see \Prado\Util\Clock\TMonotonicClock::seconds()} unlinked seed peers were last re-dialed, or null before the first. */
	private ?float $_lastRedial = null;

	/** @var array<string, true> The nodes heard this tick whose presence this node does not hold, to request in one {@see TWebSocketEnvelope::NODE_SYNC}. */
	private array $_syncRequests = [];

	/** @var bool Whether a peer requested this node's presence since its last re-announce. */
	private bool $_announcePending = false;

	/** @var ?float The {@see \Prado\Util\Clock\TMonotonicClock::seconds()} of the last requested re-announce, or null before the first. */
	private ?float $_lastAnnounce = null;

	/** @var array<string, mixed> The `ssl` stream context options for a `tls://` or `ssl://` dial. */
	private array $_sslContext = [];

	/** @var string The PCRE an announced peer URI must match to be dialed; '' dials any. */
	private string $_peerPattern = '';

	/** @var int The maximum message size of a peer link, in bytes; 0 for unlimited. */
	private int $_linkMaxMessageSize = TWebSocketConnection::DEFAULT_MAX_MESSAGE_SIZE;

	/** @var int The maximum queued outbound bytes of a peer link; 0 for unlimited. */
	private int $_linkMaxSendBufferBytes = TWebSocketConnection::DEFAULT_MAX_SEND_BUFFER;

	/** @var string This backplane's incarnation, announced in each node announce; '' until first used. */
	private string $_incarnation = '';

	/** @var array<string, string> The last incarnation announced by each node, keyed by node id. */
	private array $_incarnations = [];

	/** @var int The generation of this node's last full presence; 0 before the first. */
	private int $_generation = 0;

	/** @var array<string, int> The newest presence generation heard from each node, keyed by node id. */
	private array $_generations = [];

	/** @var array<string, array<string, int>> The clients held for each other node, keyed by node id then client id, with the generation that last set each; 0 for a client set without one. */
	private array $_remote = [];

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
	 * mesh tolerates a partial topology, and is re-dialed by {@see tick()} once per TTL.
	 * @throws TConfigurationException When no {@see getSecret() Secret} is set: nothing could join, so the mesh would be silently dead.
	 */
	public function open(): void
	{
		if ($this->_secret === '') {
			throw new TConfigurationException('websocket_backplane_mesh_secret_required');
		}
		$this->dialSeeds();
	}

	/**
	 * Dials every seed peer not already linked or being dialed.
	 */
	private function dialSeeds(): void
	{
		foreach ($this->_seedPeers as $uri) {
			try {
				$this->connectPeer($uri);
			} catch (\Throwable $e) {
				unset($this->_knownUris[$uri]);   // a seed that is down is re-dialed later, or reached via gossip
				Prado::log("Mesh seed peer {$uri} could not be dialed: " . $e->getMessage(), TLogger::NOTICE, static::class);
			}
		}
	}

	/**
	 * Re-dials the seed peers that are not linked, throttled to once per {@see getNodeTtl() TTL}, so
	 * a seed that was down at start or whose link was dropped is reached again.
	 */
	private function redialSeeds(): void
	{
		$now = $this->getMonotonicClock()->seconds();
		if ($this->_lastRedial !== null && ($now - $this->_lastRedial) < $this->_nodeTtl) {
			return;
		}
		$this->_lastRedial = $now;
		$this->dialSeeds();
	}

	/**
	 * Leaves the mesh: closes every peer link and in-flight dial, kills the hostname lookups still
	 * running, and waits up to {@see RESOLVER_REAP_TIME} seconds for those children to exit.
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
		foreach ($this->_resolving as $lookup) {
			if ($lookup['process'] !== null) {
				$this->abandonResolver($lookup['process']);
			}
		}
		$this->reapResolvers(self::RESOLVER_REAP_TIME);
		$this->_resolving = [];
		$this->_resolved = [];
		$this->_peers = [];
		$this->_pending = [];
		$this->_seen = [];
		$this->_knownUris = [];
		$this->_connectedUris = [];
		$this->_nodeSeen = [];
		$this->_incarnations = [];
		$this->_generations = [];
		$this->_remote = [];
		$this->_syncRequests = [];
		$this->_announcePending = false;
	}

	/**
	 * Flushes each peer link's queued output, drains each link, delivering received envelopes and
	 * re-flooding them, and drops a peer whose link has closed.  Then it requests the presence of the
	 * nodes newly heard, answers a request for this node's presence, and runs the heartbeat, failure
	 * detector, and seed re-dial.
	 */
	public function tick(): void
	{
		if ($this->_cluster === null) {
			return;
		}
		$this->advanceResolvers();
		$this->advancePending();
		foreach ($this->_peers as $id => $peer) {
			if (!$peer['verified'] && $this->getMonotonicClock()->seconds() > $peer['authDeadline']) {
				$this->dropPeer($id, 'the challenge went unanswered');
				continue;
			}
			if (!$this->flushPeer($id)) {
				continue;
			}
			$bytes = $this->readPeer($peer['transport']);
			if ($bytes === null || $peer['link']->getIsClosed()) {
				$this->dropPeer($id, 'the link closed');
				continue;
			}
			if ($bytes !== '') {
				$this->ingest($id, $bytes);
			}
		}
		$this->requestPresence();
		$this->reannounce();
		$this->heartbeat();
		$this->detectFailures();
		$this->redialSeeds();
	}

	/**
	 * Floods one {@see TWebSocketEnvelope::NODE_SYNC} naming the nodes heard this tick whose presence
	 * this node does not hold.
	 */
	private function requestPresence(): void
	{
		if ($this->_syncRequests === []) {
			return;
		}
		$nodes = array_map('strval', array_keys($this->_syncRequests));
		$this->_syncRequests = [];
		$envelope = new TWebSocketEnvelope(TWebSocketEnvelope::NODE_SYNC, $this->nodeId(), '', null, null, ['nodes' => $nodes]);
		$this->markSeen($envelope->getId());
		$this->flood($envelope, null);
	}

	/**
	 * Floods this node's full presence when a peer requested it, at most once per
	 * {@see ANNOUNCE_INTERVAL} after the last one; a request in between waits for the interval to pass.
	 */
	private function reannounce(): void
	{
		$now = $this->getMonotonicClock()->seconds();
		if (!$this->_announcePending || ($this->_lastAnnounce !== null && ($now - $this->_lastAnnounce) < self::ANNOUNCE_INTERVAL)) {
			return;
		}
		$this->announcePresence();
	}

	/**
	 * Floods this node's full presence under a new generation: an opening announce, each local
	 * client's presence, and a closing announce, on which every node drops the clients it holds for
	 * this node that the generation did not set.  The full presence is flooded on every link, as
	 * each later presence change is, so every node receives the node's presence envelopes in the order
	 * the node sent them.
	 */
	private function announcePresence(): void
	{
		$generation = (string) ++$this->_generation;
		$this->_announcePending = false;
		$this->_lastAnnounce = $this->getMonotonicClock()->seconds();
		$this->originate($this->nodeAnnounce(self::PRESENCE_BEGIN));
		foreach ($this->_localPresence as $clientId => $meta) {
			$this->originate(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, $this->nodeId(), $generation, null, (string) $clientId, $meta));
		}
		$this->originate($this->nodeAnnounce(self::PRESENCE_END));
	}

	/**
	 * Floods an envelope this node originates to every peer, remembering its id so a copy that returns
	 * over another path is dropped.
	 * @param TWebSocketEnvelope $envelope The envelope to originate.
	 */
	private function originate(TWebSocketEnvelope $envelope): void
	{
		$this->markSeen($envelope->getId());
		$this->flood($envelope, null);
	}

	/**
	 * Floods a node-alive heartbeat to every peer, throttled to a third of the {@see getNodeTtl() TTL}.
	 * Every node hears every other node's heartbeat, so each independently tracks the whole cluster's
	 * liveness — a single dropped link is not mistaken for a node failure in a multi-path mesh.
	 */
	private function heartbeat(): void
	{
		$now = $this->getMonotonicClock()->seconds();
		if ($this->_lastHeartbeat !== null && ($now - $this->_lastHeartbeat) < ($this->_nodeTtl / 3)) {
			return;
		}
		$this->_lastHeartbeat = $now;
		$envelope = $this->nodeAnnounce();
		$this->markSeen($envelope->getId());
		$this->flood($envelope, null);
	}

	/**
	 * Reaps nodes not heard from within the TTL, throttled to once per TTL.  A node that stopped
	 * heartbeating is declared down: its clients are dropped from the coordinator's mirror, so a
	 * crashed peer's clients do not linger as phantom presence, and any direct link to it is dropped,
	 * so a half-open socket does not hold its URI as "connected" and the node is re-dialed when it
	 * returns.  When the node is heard again, directly or through another node, this node requests its
	 * presence ({@see requestPresence()}), so its clients are re-learned.
	 */
	private function detectFailures(): void
	{
		$now = $this->getMonotonicClock()->seconds();
		if ($this->_lastNodeScan !== null && ($now - $this->_lastNodeScan) < $this->_nodeTtl) {
			return;
		}
		$this->_lastNodeScan = $now;
		foreach ($this->_nodeSeen as $node => $seen) {
			if (($now - $seen) > $this->_nodeTtl) {
				unset($this->_nodeSeen[$node], $this->_incarnations[$node], $this->_generations[$node], $this->_remote[$node]);
				Prado::log("Mesh node {$node} was not heard from for {$this->_nodeTtl}s; declaring it down", TLogger::WARNING, static::class);
				$this->_cluster?->dropNodePresence($node);
				foreach ($this->_peers as $id => $peer) {
					if ($peer['nodeId'] === $node) {
						$this->dropPeer($id, 'its node was declared down');
					}
				}
			}
		}
	}

	/**
	 * Returns the peer and in-flight-dial transports, and the stdout of each hostname lookup, to fold
	 * into the server's event loop.
	 * @return \Prado\IO\IResource[] The transports and pipes to select on.
	 */
	public function getSources(): array
	{
		$sources = array_merge(array_column($this->_peers, 'transport'), array_column($this->_pending, 'transport'));
		foreach ($this->_resolving as $lookup) {
			$stdout = $lookup['process']?->getStdout();
			if ($stdout !== null && is_resource($stdout->getResource())) {
				$sources[] = $stdout;
			}
		}
		return $sources;
	}

	/**
	 * Returns the transports of the peer links with output queued and of the dials still connecting,
	 * whose connect completes when the socket becomes writable.  Implements
	 * {@see IWebSocketBufferedBackplane}.
	 * @return IResource[] The transports to watch for writability.
	 * @since 1.2.0
	 */
	public function getWriteSources(): array
	{
		$sources = [];
		foreach ($this->_peers as $peer) {
			if ($peer['link']->hasPendingOutbound()) {
				$sources[] = $peer['transport'];
			}
		}
		foreach ($this->_pending as $pending) {
			if ($pending['state'] === 'connecting') {
				$sources[] = $pending['transport'];
			}
		}
		return $sources;
	}

	/**
	 * Writes a writable peer link's queued output, dropping the peer when the write fails, or advances a
	 * writable dial past its connect.  Implements {@see IWebSocketBufferedBackplane}.
	 * @param IResource $source The writable peer or dial transport.
	 * @since 1.2.0
	 */
	public function flushSource(IResource $source): void
	{
		foreach ($this->_peers as $id => $peer) {
			if ($peer['transport'] === $source) {
				$this->flushPeer($id);
				return;
			}
		}
		$id = spl_object_id($source);
		if (isset($this->_pending[$id]) && $this->_pending[$id]['transport'] === $source) {
			$this->advanceDial($id);
		}
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
	 * Records a local client and floods its presence to the mesh.  The envelope is encoded before
	 * anything changes, so metadata that cannot be encoded is refused to the caller and no link is
	 * touched.
	 * @param string $clientId The cluster client id.
	 * @param array<string, mixed> $meta The presence metadata (already carries the node id).
	 * @throws TWebSocketException When the metadata cannot be encoded, such as a non-finite float.
	 */
	public function putPresence(string $clientId, array $meta): void
	{
		$envelope = new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, $this->nodeId(), (string) $this->_generation, null, $clientId, $meta);
		$data = $envelope->encode();
		$this->_localPresence[$clientId] = $meta;
		$this->markSeen($envelope->getId());
		$this->floodEncoded($data, null);
	}

	/**
	 * Forgets a local client and floods its departure to the mesh.
	 * @param string $clientId The cluster client id.
	 */
	public function dropPresence(string $clientId): void
	{
		unset($this->_localPresence[$clientId]);
		$this->originate(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_DROP, $this->nodeId(), (string) $this->_generation, null, $clientId));
	}

	// =========================================================================
	// Peers
	// =========================================================================

	/**
	 * Adds an established peer link and, on a secret-gated mesh, begins mutually authenticating it.
	 * A known remote URI is recorded so the same peer is never linked twice.
	 *
	 * Authentication is mutual: this node challenges the peer with a fresh nonce and withholds its
	 * full presence — and every other cluster message — until it has verified the peer's answer.  Because
	 * both a dialed and an accepted peer are challenged this way, and the dialer no longer trusts an
	 * endpoint merely because it dialed it, a node a peer dials cannot harvest that node's presence or
	 * advertised URI without proving the secret.  The answers are ordered: the dialer answers at once,
	 * and an accepted peer's challenge is answered only after that peer is verified
	 * ({@see answerChallenge()}).
	 * @param TWebSocketConnection $link The peer connection.
	 * @param TSocketStream $transport The connection's transport, for the event loop.
	 * @param string $uri The remote's URI when known (a dialed peer), or '' for an accepted peer.
	 */
	public function addPeer(TWebSocketConnection $link, TSocketStream $transport, string $uri = ''): void
	{
		$peerId = spl_object_id($link);
		$this->_peers[$peerId] = ['link' => $link, 'transport' => $transport, 'uri' => $uri, 'aliases' => [], 'verified' => true, 'nonce' => '', 'heldNonce' => '', 'authDeadline' => 0.0, 'nodeId' => '', 'limit' => $link->getMaxMessageSize()];
		if ($uri !== '') {
			$this->_connectedUris[$uri] = true;
			$this->_knownUris[$uri] = true;
		}
		if ($this->_secret === '') {
			$this->linkUp($peerId);   // without a secret no network path is open ({@see open()} refuses), so a link added programmatically is trusted as-is
			return;
		}
		// An inbound peer is subject to the pending-peer cap: a replayed handshake proof passes
		// authenticate() repeatedly and accepted peers do not count against the server's connection
		// limit, so this bounds how many half-authenticated links a flood can hold at once.
		if (!$link->getIsClient() && $this->_maxPendingPeers > 0 && $this->countUnverifiedPeers() >= $this->_maxPendingPeers) {
			unset($this->_peers[$peerId]);
			try {
				$link->close();
			} catch (\Throwable $e) {
				// The peer is already gone; nothing to close cleanly.
			}
			Prado::log("Mesh shed an inbound peer: {$this->_maxPendingPeers} peers are already mid-authentication", TLogger::WARNING, static::class);
			return;   // shed: too many peers are mid-authentication (a replayed-proof connection flood)
		}
		// Challenge the peer and withhold trust (and all state) until it answers.
		$nonce = bin2hex(random_bytes(16));
		$this->_peers[$peerId]['verified'] = false;
		$this->_peers[$peerId]['nonce'] = $nonce;
		$this->_peers[$peerId]['authDeadline'] = $this->getMonotonicClock()->seconds() + $this->_timeout;
		$limit = $link->getMaxMessageSize();
		if (!$link->getIsClient() && ($limit === 0 || $limit > self::UNVERIFIED_MESSAGE_SIZE)) {
			$link->setMaxMessageSize(self::UNVERIFIED_MESSAGE_SIZE);   // an unproven accepted peer sends only the challenge exchange; its limit is restored on verification
		}
		$this->sendTo($link, new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_CHALLENGE, $this->nodeId(), $nonce));
	}

	/**
	 * Brings a verified link into the mesh.  This node floods its full presence
	 * ({@see announcePresence()}), which reaches the new peer and, through the other links, the nodes
	 * the link reconnects.  It then asks the peer for the presence of the nodes it has heard and holds
	 * no link to: a missing link can have kept their presence changes from this node.  The peer relays
	 * the request to them.  On a secret-gated mesh this runs only from {@see verifyChallenge()}, once
	 * this node has verified the peer, so state is never shown to an unproven peer.
	 * @param int $peerId The peer object id of the link.
	 */
	private function linkUp(int $peerId): void
	{
		$this->announcePresence();
		if (!isset($this->_peers[$peerId])) {
			return;   // the link failed while the presence was sent
		}
		$linked = array_flip(array_column($this->_peers, 'nodeId'));
		$nodes = array_diff_key($this->_nodeSeen, $linked, [$this->nodeId() => true]);
		if ($nodes === []) {
			return;
		}
		$request = new TWebSocketEnvelope(TWebSocketEnvelope::NODE_SYNC, $this->nodeId(), '', null, null, ['nodes' => array_map('strval', array_keys($nodes))]);
		$this->markSeen($request->getId());
		$this->sendTo($this->_peers[$peerId]['link'], $request);
	}

	/**
	 * Closes the surplus links to a node once a link to it is verified, so two nodes keep one link.
	 * Only a link this node dialed is closed here, and the peer closes those it dialed, so a 1.1.0 peer
	 * is never dropped for a link it will dial again.  Of the links this node dialed, the oldest
	 * remains.  When the peer dialed one too, the link dialed by the node with the lower node id
	 * remains.  {@see dropPeer()} keeps a closed link's URI bound to the link that remains.
	 * @param string $node The peer's node id.
	 */
	private function closeSurplusLinks(string $node): void
	{
		$dialed = [];
		$accepted = false;
		foreach ($this->_peers as $id => $peer) {
			if (!$peer['verified'] || $peer['nodeId'] !== $node) {
				continue;
			}
			if ($peer['link']->getIsClient()) {
				$dialed[] = $id;
			} else {
				$accepted = true;
			}
		}
		if (!$accepted || strcmp($this->nodeId(), $node) < 0) {
			array_shift($dialed);   // this node's oldest dial remains
		}
		foreach ($dialed as $id) {
			$this->dropPeer($id, "another link reaches node {$node}");
		}
	}

	/**
	 * Returns the oldest verified link to a node.
	 * @param string $node The node id.
	 * @return ?int The link's peer object id, or null when no verified link reaches the node.
	 */
	private function linkTo(string $node): ?int
	{
		if ($node === '') {
			return null;
		}
		foreach ($this->_peers as $id => $peer) {
			if ($peer['verified'] && $peer['nodeId'] === $node) {
				return $id;
			}
		}
		return null;
	}

	/**
	 * Records a URI as reaching a linked peer, so it is not dialed while the link stands.  The first
	 * URI is the link's own; a further one is an alias.
	 * @param int $peerId The peer object id of the link.
	 * @param string $uri The URI that reaches the peer's node.
	 */
	private function bindUri(int $peerId, string $uri): void
	{
		if ($this->_peers[$peerId]['uri'] === '') {
			$this->_peers[$peerId]['uri'] = $uri;
		} elseif ($this->_peers[$peerId]['uri'] !== $uri) {
			$this->_peers[$peerId]['aliases'][$uri] = true;
		}
		$this->_connectedUris[$uri] = true;
		$this->_knownUris[$uri] = true;
	}

	/**
	 * Builds this node's announce: a {@see TWebSocketEnvelope::NODE_UP} carrying its advertised URI,
	 * its incarnation, and the generation of its last full presence.
	 * @param ?string $presence {@see PRESENCE_BEGIN} or {@see PRESENCE_END} for the announce that opens or closes a full presence; null for a heartbeat.
	 * @return TWebSocketEnvelope The announce.
	 */
	private function nodeAnnounce(?string $presence = null): TWebSocketEnvelope
	{
		if ($this->_incarnation === '') {
			$this->_incarnation = bin2hex(random_bytes(8));
		}
		$meta = ['uri' => $this->_advertise, 'incarnation' => $this->_incarnation, 'generation' => $this->_generation];
		if ($presence !== null) {
			$meta['presence'] = $presence;
		}
		return new TWebSocketEnvelope(TWebSocketEnvelope::NODE_UP, $this->nodeId(), '', null, null, $meta);
	}

	/**
	 * Records a node's announced incarnation.  A changed incarnation means the node restarted under
	 * the same id, so the clients held for its previous incarnation are dropped from the presence
	 * mirror.  An announce without an incarnation (from a node predating it) is ignored.
	 * @param string $node The announcing node id.
	 * @param string $incarnation The announced incarnation, or ''.
	 */
	private function trackIncarnation(string $node, string $incarnation): void
	{
		if ($incarnation === '') {
			return;
		}
		$known = $this->_incarnations[$node] ?? '';
		$this->_incarnations[$node] = $incarnation;
		if ($known !== '' && $known !== $incarnation) {
			Prado::log("Mesh node {$node} restarted with a new incarnation; dropping its previous clients", TLogger::NOTICE, static::class);
			unset($this->_generations[$node], $this->_remote[$node]);   // the new incarnation counts its generations from the start
			$this->_cluster?->dropNodePresence($node);
		}
	}

	/**
	 * Answers a peer's challenge by signing the nonce together with this node's id.  Answering only
	 * proves this node knows the secret; it grants no trust and releases no state, which happens in
	 * {@see verifyChallenge()} when this node has verified the peer's answer to its own nonce.
	 *
	 * The dialing side answers at once.  The accepting side holds an unverified peer's challenge and
	 * answers it from {@see verifyChallenge()}, once the peer has proven the secret.  Otherwise any
	 * accepted link, such as a replayed handshake, could have this node sign a nonce another node
	 * issued, and present the signature to that node as this node's answer.
	 *
	 * A challenge that repeats a nonce this node itself issued is a reflection: the peer is trying to
	 * have this node sign its own challenge and hand the answer back.  Such a peer is dropped.  The
	 * node id in the answer closes the same hole structurally, since {@see verifyChallenge()} refuses
	 * an answer claiming this node's own id.
	 * @param int $peerId The peer the challenge arrived on.
	 * @param string $nonce The challenge nonce.
	 */
	private function answerChallenge(int $peerId, string $nonce): void
	{
		if ($nonce === '' || $this->_secret === '' || !isset($this->_peers[$peerId])) {
			return;
		}
		foreach ($this->_peers as $peer) {
			if ($peer['nonce'] !== '' && hash_equals($peer['nonce'], $nonce)) {
				$this->dropPeer($peerId, 'it reflected this node\'s own challenge');   // never sign a nonce this node issued
				return;
			}
		}
		$link = $this->_peers[$peerId]['link'];
		if (!$link->getIsClient() && !$this->_peers[$peerId]['verified']) {
			$this->_peers[$peerId]['heldNonce'] = $nonce;   // an accepted peer proves the secret first; verifyChallenge() answers
			return;
		}
		$this->sendTo($link, new TWebSocketEnvelope(TWebSocketEnvelope::AUTH_RESPONSE, $this->nodeId(), $this->challengeAnswer($nonce, $this->nodeId())));
	}

	/**
	 * Verifies a peer's answer to this node's challenge: a correct HMAC of the nonce and the peer's
	 * node id marks the peer trusted, releasing the withheld presence and letting its traffic flow,
	 * while a wrong or missing answer drops the peer.  An answer claiming this node's own id is
	 * refused outright, so an answer this node produced can never verify it to itself.  Trust is
	 * per-direction: the peer independently verifies this node before it trusts this node in turn.
	 * @param int $peerId The peer the answer arrived on.
	 * @param string $response The peer's HMAC of the nonce and its node id.
	 * @param string $answerer The node id the answer claims, from the envelope origin.
	 */
	private function verifyChallenge(int $peerId, string $response, string $answerer): void
	{
		if (!isset($this->_peers[$peerId])) {
			return;
		}
		$nonce = $this->_peers[$peerId]['nonce'];
		if ($nonce === '') {
			return;   // no outstanding challenge (already verified); ignore a stray or duplicate answer rather than drop a good peer
		}
		if ($answerer !== '' && $answerer !== $this->nodeId() && $response !== '' && hash_equals($this->challengeAnswer($nonce, $answerer), $response)) {
			$this->_peers[$peerId]['verified'] = true;
			$this->_peers[$peerId]['nonce'] = '';
			$this->_peers[$peerId]['authDeadline'] = 0.0;
			$this->_peers[$peerId]['nodeId'] = $answerer;
			$this->_peers[$peerId]['link']->setMaxMessageSize($this->_peers[$peerId]['limit']);
			$this->_nodeSeen[$answerer] = $this->getMonotonicClock()->seconds();   // answering the challenge is proof of life, before its first heartbeat; its full presence follows
			$held = $this->_peers[$peerId]['heldNonce'];
			if ($held !== '') {
				$this->_peers[$peerId]['heldNonce'] = '';
				$this->answerChallenge($peerId, $held);   // the accepted peer is verified: answer its challenge, ahead of the presence
			}
			$this->closeSurplusLinks($answerer);
			if (isset($this->_peers[$peerId])) {   // not the surplus link itself
				$this->linkUp($peerId);
			}
		} else {
			$this->dropPeer($peerId, 'it failed the challenge');   // not a legitimate peer
		}
	}

	/**
	 * Indicates whether an upgrade targets the mesh peer path, so the server hands it here as a peer
	 * link rather than a client.  Implements {@see \Prado\IO\Socket\WebSocket\IWebSocketEndpoint}.
	 * @param ?string $target The request target (path).
	 * @return bool Whether the target is the mesh {@see getPath() Path}.
	 */
	public function matchesTarget(?string $target): bool
	{
		return $this->_secret !== '' && $target !== null && $target === $this->_path;   // the endpoint is exposed only when a secret enables the mesh
	}

	/**
	 * Joins an authenticated, upgraded connection to the mesh as a peer.  Implements
	 * {@see \Prado\IO\Socket\WebSocket\IWebSocketEndpoint}.
	 * @param TWebSocketConnection $connection The upgraded peer connection.
	 * @param TSocketStream $transport The connection transport.
	 * @param array<string, mixed> $request The parsed upgrade request.
	 */
	public function accept(TWebSocketConnection $connection, TSocketStream $transport, array $request): void
	{
		$this->addPeer($connection, $transport);
	}

	/**
	 * Begins dialing a peer without blocking: it opens an asynchronous TCP connection and registers the
	 * attempt, which advances through the connect, the TLS handshake for a `tls://` or `ssl://` URI,
	 * and the WebSocket handshake before adding the link.  A blocking dial would stall the serve loop,
	 * so discovery and reconnection stay async.  A hostname in the URI is looked up first, in a child
	 * process ({@see startResolver()}), and the dial starts when the lookup answers.  A URI that is
	 * linked, being dialed or being looked up, or whose scheme is not `tcp`, `tls` or `ssl`, is ignored.
	 * @param string $uri The peer endpoint, e.g. 'tcp://10.0.0.2:8080', 'tls://node-b.internal:8443' or 'tcp://[fe80::1%eth0]:8080'.
	 * @throws \Prado\Exceptions\TSocketException When the connection cannot be started.
	 * @throws TInvalidDataValueException When the URI's host is neither a hostname nor an IP address.
	 * @throws TConfigurationException When the host needs a lookup and no {@see getResolverBinary() ResolverBinary} is available.
	 * @throws \Prado\Exceptions\TIOException When the lookup's child process cannot be started.
	 */
	public function connectPeer(string $uri): void
	{
		if ($this->_secret === '' || isset($this->_connectedUris[$uri])) {
			return;   // the mesh is disabled without a secret, or already linked to this peer
		}
		if (!preg_match('#^(tcp|tls|ssl)://#i', $uri)) {
			return;   // only dial stream sockets; refuse gossiped non-socket URIs (SSRF hardening)
		}
		foreach ($this->_pending as $pending) {
			if ($pending['uri'] === $uri) {
				return;   // a dial to this peer is already in flight; do not open a duplicate link
			}
		}
		$address = TSocketAddress::parse($uri);
		$host = trim((string) $address->getHost(), '[]');
		if (isset($this->_resolving[$host]['uris'][$uri])) {
			return;   // waiting on the lookup of its host
		}
		$ip = $this->addressOf($host);
		if ($ip === null) {
			$this->resolve($host, $uri);   // connectPeer() runs again once the host resolves
			return;
		}
		$this->_knownUris[$uri] = true;
		$secure = !preg_match('#^tcp://#i', $uri);
		$context = null;
		// Connect to the address over TCP: stream_socket_client() would resolve a hostname, and run a
		// TLS handshake inside the connect, both blocking; advanceDial() drives the TLS handshake.
		$target = 'tcp://' . (str_contains($ip, ':') ? '[' . $ip . ']' : $ip) . ':' . $address->getPort();
		if ($secure) {
			$context = stream_context_create(['ssl' => $this->_sslContext + ['peer_name' => explode('%', $host, 2)[0]]]);   // the name the certificate is verified against: the URI's host, never the resolved address
		}
		$transport = TSocketStream::connect($target, $this->_timeout, STREAM_CLIENT_ASYNC_CONNECT | STREAM_CLIENT_CONNECT, $context);
		$transport->setBlocking(false);
		$this->_pending[spl_object_id($transport)] = [
			'transport' => $transport,
			'uri' => $uri,
			'state' => 'connecting',
			'secure' => $secure,
			'key' => '',
			'buffer' => '',
			'deadline' => $this->getMonotonicClock()->seconds() + $this->_timeout,
		];
	}

	/**
	 * Verifies an inbound peer handshake against the shared secret.  The proof is an HMAC of the
	 * connection's `Sec-WebSocket-Key` keyed by the secret, so the secret never crosses the wire and a
	 * proof lifted onto a different handshake key is rejected.  The mesh is disabled without a secret,
	 * so an unauthenticated peer can never join.
	 *
	 * This handshake proof is a bearer credential a wire-tap could replay, so it is only the first
	 * gate: once upgraded, the accepting node issues a fresh-nonce challenge ({@see addPeer()}) that a
	 * replayed handshake cannot answer without the secret.  The proof and the challenge answer are
	 * domain-separated ({@see authToken()}), so a node that a peer dials cannot be used as a signing
	 * oracle to turn a challenge answer into a forged handshake proof.  Still prefer a `tls://`
	 * transport on any untrusted network so traffic and presence stay confidential.
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
		return $key !== '' && $provided !== '' && hash_equals($this->authToken(self::AUTH_CONTEXT_HANDSHAKE, $key), $provided);
	}

	/**
	 * Computes an authentication token for a value under the shared secret, bound to a purpose.  The
	 * `$context` tag domain-separates the two uses — the handshake proof over the `Sec-WebSocket-Key`
	 * and the challenge answer over the nonce — so a token issued for one purpose can never satisfy the
	 * other.  Without this separation, a malicious node a peer dials could harvest challenge answers for
	 * chosen values and replay them as handshake proofs, defeating the secret.
	 * @param string $context The purpose tag ({@see AUTH_CONTEXT_HANDSHAKE} or {@see AUTH_CONTEXT_CHALLENGE}).
	 * @param string $value The value to sign (the handshake key, or the nonce).
	 * @return string The base64 HMAC-SHA256 of the context-tagged value under the shared secret.
	 */
	private function authToken(string $context, string $value): string
	{
		return base64_encode(hash_hmac('sha256', $context . ':' . $value, sha1($this->_secret), true));
	}

	/**
	 * Computes the answer to a challenge: the {@see AUTH_CONTEXT_CHALLENGE challenge-tagged} token over
	 * the nonce and the answering node's id, so an answer is bound to who gave it.
	 * @param string $nonce The challenge nonce.
	 * @param string $answerer The id of the node answering.
	 * @return string The answer token.
	 */
	private function challengeAnswer(string $nonce, string $answerer): string
	{
		return $this->authToken(self::AUTH_CONTEXT_CHALLENGE, $nonce . self::AUTH_ANSWER_SEPARATOR . $answerer);
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

	/**
	 * Returns the number of accepted peers that have not yet answered their challenge.
	 * @return int The unverified peer count.
	 */
	private function countUnverifiedPeers(): int
	{
		$count = 0;
		foreach ($this->_peers as $peer) {
			if (!$peer['verified']) {
				$count++;
			}
		}
		return $count;
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
		$type = $envelope->getType();
		if ($type === TWebSocketEnvelope::AUTH_CHALLENGE) {
			$this->answerChallenge($fromPeerId, $envelope->getPayload());
			return;   // a point-to-point challenge, never flooded or deduplicated
		}
		if ($type === TWebSocketEnvelope::AUTH_RESPONSE) {
			$this->verifyChallenge($fromPeerId, $envelope->getPayload(), $envelope->getOriginNode());
			return;   // a point-to-point answer, never flooded or deduplicated
		}
		if (isset($this->_peers[$fromPeerId]) && !$this->_peers[$fromPeerId]['verified']) {
			return;   // a challenged peer's traffic is ignored until it proves the secret
		}
		$origin = $envelope->getOriginNode();
		if ($origin !== '' && $origin !== $this->nodeId()) {
			if (!isset($this->_nodeSeen[$origin]) && !($type === TWebSocketEnvelope::NODE_UP && ($envelope->getMeta()['presence'] ?? null) === self::PRESENCE_BEGIN)) {
				$this->_syncRequests[$origin] = true;   // a node never heard, or declared down, is heard: ask for its presence, unless this opens it
			}
			$this->_nodeSeen[$origin] = $this->getMonotonicClock()->seconds();   // any traffic (data or heartbeat) proves the origin node is alive, even a duplicate copy
		}
		if (isset($this->_seen[$envelope->getId()])) {
			return;
		}
		if ($origin === $this->nodeId()) {
			// This node marks its own envelopes seen before flooding them, so an unseen envelope under its
			// id comes from another node configured with the same NodeId: the two would drop each other's
			// traffic as echo.  Report it and ignore the envelope rather than route it.
			Prado::log("Mesh received traffic from another node using this node's id '{$origin}'; check the NodeId configuration", TLogger::WARNING, static::class);
			return;
		}
		$this->markSeen($envelope->getId());
		$meta = $envelope->getMeta();
		if ($type === TWebSocketEnvelope::NODE_SYNC) {
			$nodes = $meta['nodes'] ?? [];
			if (is_array($nodes) && in_array($this->nodeId(), $nodes, true)) {
				$this->_announcePending = true;   // answered by reannounce(), coalesced with other requests
			}
			$this->flood($envelope, $fromPeerId);   // a mesh request: relayed, never routed to the coordinator
			return;
		}
		$current = true;
		if ($type === TWebSocketEnvelope::NODE_UP) {
			$incarnation = $meta['incarnation'] ?? '';
			$this->trackIncarnation($origin, is_string($incarnation) ? $incarnation : '');   // before any presence the new incarnation sends
			$this->trackGeneration($origin, $meta['generation'] ?? null, ($meta['presence'] ?? null) === self::PRESENCE_END);
		} elseif ($type === TWebSocketEnvelope::PRESENCE_SET || $type === TWebSocketEnvelope::PRESENCE_DROP) {
			$current = $this->trackPresence($envelope);
		}
		if ($current) {
			try {
				$this->_cluster?->receiveEnvelope($envelope);
			} catch (\Throwable $e) {
				Prado::log('Mesh delivery of a ' . $type . ' envelope failed: ' . $e->getMessage(), TLogger::WARNING, static::class);   // one bad delivery must not stop the relay or the peer
			}
		}
		$this->flood($envelope, $fromPeerId);
		if ($type === TWebSocketEnvelope::NODE_UP) {
			$uri = $meta['uri'] ?? '';
			$this->discover($fromPeerId, $origin, is_string($uri) ? $uri : '');   // a URI that is not a string is no URI
		}
	}

	/**
	 * Records the presence generation a node announces.  The announce that closes the node's full
	 * presence of that generation completes it: the clients held for the node that no envelope of the
	 * generation set have left it, and are dropped from the presence mirror.  The clients the
	 * generation set stay throughout.  An announce without a generation (from a 1.1.0 node), or of an
	 * older generation than the newest heard, changes nothing.
	 * @param string $node The announcing node id.
	 * @param mixed $generation The announced generation.
	 * @param bool $complete Whether the announce closes a full presence.
	 */
	private function trackGeneration(string $node, mixed $generation, bool $complete): void
	{
		if (!is_int($generation) || $generation < 1 || $node === '' || $generation < ($this->_generations[$node] ?? 0)) {
			return;
		}
		$this->_generations[$node] = $generation;
		if (!$complete) {
			return;
		}
		foreach ($this->_remote[$node] ?? [] as $clientId => $set) {
			if ($set < $generation) {
				unset($this->_remote[$node][$clientId]);
				try {
					$this->_cluster?->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_DROP, $node, '', null, (string) $clientId));
				} catch (\Throwable $e) {
					Prado::log("Mesh could not drop the departed client {$clientId} of node {$node}: " . $e->getMessage(), TLogger::WARNING, static::class);
				}
			}
		}
	}

	/**
	 * Records a presence change another node originated, by the generation its payload carries.  A
	 * change of an older generation than the newest heard from the node was sent before the node's
	 * last full presence and is not applied.  A change without a generation (from a 1.1.0 node) is
	 * applied as it arrives.
	 * @param TWebSocketEnvelope $envelope The presence envelope.
	 * @return bool Whether the change is current and is delivered to the coordinator.
	 */
	private function trackPresence(TWebSocketEnvelope $envelope): bool
	{
		$node = $envelope->getOriginNode();
		$clientId = $envelope->getClientId();
		if ($node === '' || $clientId === null) {
			return true;
		}
		$payload = $envelope->getPayload();
		$newest = $this->_generations[$node] ?? 0;
		$generation = ($payload !== '' && strlen($payload) < 19 && ctype_digit($payload)) ? (int) $payload : null;
		if ($generation !== null) {
			if ($generation < $newest) {
				return false;
			}
			$this->_generations[$node] = $generation;
		}
		if ($envelope->getType() === TWebSocketEnvelope::PRESENCE_SET) {
			$this->_remote[$node][$clientId] = $generation ?? $newest;
		} else {
			unset($this->_remote[$node][$clientId]);
		}
		return true;
	}

	/**
	 * Acts on a node announce: an announce the directly-connected peer made about itself binds its
	 * URI to the link (never dialed again); an announce it relayed from a distant node names that node
	 * to learn and dial.  The two are told apart by the announce's origin against the peer's proven
	 * node id, so a relayed announce arriving first on a link cannot mis-bind a third node's URI to it.
	 * @param int $fromPeerId The peer the announce arrived on.
	 * @param string $origin The node the announce originated on.
	 * @param string $uri The advertised URI.
	 */
	private function discover(int $fromPeerId, string $origin, string $uri): void
	{
		if ($uri === '') {
			return;
		}
		$peer = $this->_peers[$fromPeerId] ?? null;
		if ($peer !== null && $peer['uri'] === '' && ($peer['nodeId'] === '' || $peer['nodeId'] === $origin)) {
			$this->bindUri($fromPeerId, $uri);
			return;
		}
		$linked = $this->linkTo($origin);
		if ($linked !== null) {
			$this->bindUri($linked, $uri);   // another name of a node this node is linked to, such as the address of a seed dialed by hostname
			return;
		}
		$this->learnPeer($uri);
	}

	/**
	 * Learns a peer URI from a gossiped announce and dials it once.  A node dials only the URIs that
	 * sort after its own {@see getAdvertise() Advertise} (or all, when it advertises nothing), so of
	 * any two nodes exactly one initiates the link.  A URI that does not match the
	 * {@see getPeerPattern() PeerPattern} is never dialed.
	 * @param string $uri The advertised peer URI.
	 */
	private function learnPeer(string $uri): void
	{
		if ($uri === '' || $uri === $this->_advertise || isset($this->_knownUris[$uri]) || isset($this->_connectedUris[$uri])) {
			return;
		}
		if ($this->_peerPattern !== '' && preg_match($this->_peerPattern, $uri) !== 1) {
			$this->_knownUris[$uri] = true;
			Prado::log("Mesh does not dial the announced peer {$uri}: it does not match the PeerPattern", TLogger::NOTICE, static::class);
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
	 * Sends an envelope to every verified peer except an optional source.  An unverified peer receives
	 * no cluster traffic — presence, publishes, broadcasts, or heartbeats — until it has proven the
	 * secret, so a peer this node dials but has not yet verified is shown nothing.
	 * @param TWebSocketEnvelope $envelope The envelope to send.
	 * @param ?int $exceptPeerId The peer id to skip (the source of a re-flood), or null.
	 */
	private function flood(TWebSocketEnvelope $envelope, ?int $exceptPeerId): void
	{
		$data = $this->encode($envelope);
		if ($data !== null) {
			$this->floodEncoded($data, $exceptPeerId);
		}
	}

	/**
	 * Sends an encoded envelope to every verified peer except an optional source.
	 * @param string $data The encoded envelope.
	 * @param ?int $exceptPeerId The peer id to skip (the source of a re-flood), or null.
	 */
	private function floodEncoded(string $data, ?int $exceptPeerId): void
	{
		foreach ($this->_peers as $id => $peer) {
			if ($id !== $exceptPeerId && $peer['verified']) {
				$this->sendEncoded($peer['link'], $data);
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
		$data = $this->encode($envelope);
		if ($data !== null) {
			$this->sendEncoded($link, $data);
		}
	}

	/**
	 * Encodes an envelope once for all the links it goes to.  An envelope that cannot be encoded is
	 * logged and not sent: the fault lies in the envelope, so no link is dropped for it.
	 * @param TWebSocketEnvelope $envelope The envelope to encode.
	 * @return ?string The encoded envelope, or null when it cannot be encoded.
	 */
	private function encode(TWebSocketEnvelope $envelope): ?string
	{
		try {
			return $envelope->encode();
		} catch (TWebSocketException $e) {
			Prado::log('Mesh cannot send a ' . $envelope->getType() . ' envelope: ' . $e->getMessage(), TLogger::WARNING, static::class);
			return null;
		}
	}

	/**
	 * Sends an encoded envelope over a peer link, dropping the peer when the send fails.
	 * @param TWebSocketConnection $link The peer connection.
	 * @param string $data The encoded envelope.
	 */
	private function sendEncoded(TWebSocketConnection $link, string $data): void
	{
		try {
			if (!$link->getIsClosed()) {
				$link->send($data);
			}
		} catch (\Throwable $e) {
			$this->dropPeer(spl_object_id($link), 'a send failed: ' . $e->getMessage());
		}
	}

	/**
	 * Writes a peer link's queued output without blocking, dropping the peer when the write fails.
	 * @param int $id The peer object id.
	 * @return bool Whether the peer is still linked.
	 */
	private function flushPeer(int $id): bool
	{
		if (!isset($this->_peers[$id])) {
			return false;
		}
		try {
			$this->_peers[$id]['link']->flushOutbound();
		} catch (\Throwable $e) {
			$this->dropPeer($id, 'a flush failed: ' . $e->getMessage());
			return false;
		}
		return true;
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
	 * Closes and forgets a peer link, logging why.  Its URIs are freed to be discovered and dialed
	 * again, unless another verified link still reaches the same node: they then stay bound, to that
	 * link.
	 * @param int $id The peer object id.
	 * @param string $reason Why the peer is dropped, for the log.
	 */
	private function dropPeer(int $id, string $reason = ''): void
	{
		if (!isset($this->_peers[$id])) {
			return;
		}
		$peer = $this->_peers[$id];
		$uri = $peer['uri'];
		$node = $peer['nodeId'];
		Prado::log('Mesh dropped peer ' . ($node !== '' ? $node : '(unverified)') . ($uri !== '' ? " at {$uri}" : '') . ($reason !== '' ? ": {$reason}" : ''), TLogger::NOTICE, static::class);
		try {
			$peer['link']->close();
		} catch (\Throwable $e) {
			// The peer is already gone; a clean Close frame cannot be sent.
		}
		unset($this->_peers[$id]);
		$remaining = $peer['verified'] ? $this->linkTo($node) : null;
		foreach (array_keys($peer['aliases'] + ($uri !== '' ? [$uri => true] : [])) as $bound) {
			if ($remaining !== null) {
				$this->bindUri($remaining, (string) $bound);
			} else {
				unset($this->_connectedUris[$bound], $this->_knownUris[$bound]);   // allow re-discovery and re-dial
			}
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
			// A protocol error, or an I/O failure echoing a Close to a vanished peer, drops it, after
			// the messages the same read completed before a malformed frame.
			if ($e instanceof TWebSocketException) {
				$this->route(array_map(fn ($message) => $message->getPayload(), $e->getDecodedMessages()), $peerId);
			}
			$this->dropPeer($peerId, 'its link failed: ' . $e->getMessage());
			return;
		}
		$this->route($messages, $peerId);
	}

	/**
	 * Decodes and receives the envelopes one read of a peer link delivered.
	 * @param string[] $messages The message payloads, in order.
	 * @param int $peerId The peer they came from.
	 */
	private function route(array $messages, int $peerId): void
	{
		foreach ($messages as $message) {
			if (!isset($this->_peers[$peerId])) {
				return;   // an earlier envelope dropped the peer
			}
			$envelope = TWebSocketEnvelope::decode($message);
			if ($envelope !== null) {
				$this->receive($envelope, $peerId);
			}
		}
	}

	/**
	 * Advances each in-flight dial, dropping a timed-out or failed attempt.
	 */
	private function advancePending(): void
	{
		foreach (array_keys($this->_pending) as $id) {
			$this->advanceDial($id);
		}
	}

	/**
	 * Advances one in-flight dial without blocking: a socket that has connected starts its TLS
	 * handshake (a `tls://` or `ssl://` peer) or sends the WebSocket upgrade, a TLS handshake takes its
	 * next step, and a handshaking dial reads the response.  An attempt past its deadline, or whose
	 * socket closed, is dropped.
	 * @param int $id The pending dial id.
	 */
	private function advanceDial(int $id): void
	{
		$pending = $this->_pending[$id] ?? null;
		if ($pending === null) {
			return;
		}
		$resource = $pending['transport']->getResource();
		if (!is_resource($resource) || $this->getMonotonicClock()->seconds() > $pending['deadline']) {
			$this->failPending($id);
			return;
		}
		if ($pending['state'] === 'connecting') {
			if (!$this->ready($resource, true)) {
				return;
			}
			if (@stream_socket_get_name($resource, true) === false) {
				$this->failPending($id);   // the connection attempt did not succeed
				return;
			}
			if (!$pending['secure']) {
				$this->sendPeerHandshake($id);
				return;
			}
			$this->_pending[$id]['state'] = 'securing';
		}
		if ($this->_pending[$id]['state'] === 'securing') {
			$this->secureDial($id);
		} elseif ($this->ready($resource, false)) {
			$this->readPeerHandshake($id);
		}
	}

	/**
	 * Takes the next non-blocking step of a dial's TLS handshake, sending the WebSocket upgrade once it
	 * completes.  A failed handshake (an untrusted certificate, a peer that is not TLS) drops the dial.
	 * @param int $id The pending dial id.
	 */
	private function secureDial(int $id): void
	{
		try {
			$result = $this->_pending[$id]['transport']->enableCrypto(true, STREAM_CRYPTO_METHOD_TLS_CLIENT);
		} catch (\Throwable $e) {
			$result = false;   // PRADO's error handler raises the TLS warning
		}
		if ($result === true) {
			$this->sendPeerHandshake($id);
		} elseif ($result === false) {
			$this->failPending($id);
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
	 * Sends the WebSocket upgrade request once a dialed socket has connected (and, for a `tls://` or
	 * `ssl://` peer, completed its TLS handshake).
	 * @param int $id The pending dial id.
	 */
	private function sendPeerHandshake(int $id): void
	{
		$transport = $this->_pending[$id]['transport'];
		$key = TWebSocketHandshake::generateKey();
		$headers = $this->_secret === '' ? [] : [self::AUTH_HEADER => $this->authToken(self::AUTH_CONTEXT_HANDSHAKE, $key)];
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
		$link->setMaxMessageSize($this->_linkMaxMessageSize);
		$link->setMaxSendBufferBytes($this->_linkMaxSendBufferBytes);
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
		Prado::log("Mesh dial to {$uri} failed in state '{$this->_pending[$id]['state']}'", TLogger::NOTICE, static::class);
		$this->_pending[$id]['transport']->close();
		unset($this->_pending[$id]);
		unset($this->_knownUris[$uri]);
		unset($this->_resolved[trim((string) TSocketAddress::parse($uri)->getHost(), '[]')]);   // a peer that moved is looked up again
	}

	// =========================================================================
	// Hostname resolution
	// =========================================================================

	/**
	 * Returns the address to connect to for a host: the host itself when it is an IP address, with or
	 * without a zone, or a cached lookup that has not expired.
	 * @param string $host The URI host, without IPv6 brackets.
	 * @return ?string The IP address, or null when the host must be looked up.
	 */
	private function addressOf(string $host): ?string
	{
		if (filter_var($host, FILTER_VALIDATE_IP) !== false || $this->isScopedAddress($host)) {
			return $host;
		}
		$cached = $this->_resolved[$host] ?? null;
		if ($cached !== null && $this->getMonotonicClock()->seconds() < $cached['expires']) {
			return $cached['address'];
		}
		unset($this->_resolved[$host]);
		return null;
	}

	/**
	 * Indicates whether a host is an IPv6 address with a zone, such as `fe80::1%eth0`.  The system
	 * connects to such an address as written, so it needs no lookup.
	 * @param string $host The URI host, without IPv6 brackets.
	 * @return bool Whether the host is a zoned IPv6 address.
	 */
	private function isScopedAddress(string $host): bool
	{
		return preg_match(self::SCOPED_ADDRESS_PATTERN, $host, $match) === 1 && filter_var($match[1], FILTER_VALIDATE_IP, FILTER_FLAG_IPV6) !== false;
	}

	/**
	 * Starts looking up a host for a dial, or adds the dial to the lookup of its host.  At most
	 * {@see MAX_RESOLVERS} lookups run at once; a further host waits its turn, which {@see tick()}
	 * gives it.  A host that is not a valid DNS name is refused without a lookup.
	 * @param string $host The hostname.
	 * @param string $uri The peer URI waiting on it.
	 * @throws TInvalidDataValueException When the host is not a valid hostname.
	 * @throws TConfigurationException When no {@see getResolverBinary() ResolverBinary} is available.
	 * @throws \Prado\Exceptions\TIOException When the lookup's child process cannot be started.
	 */
	private function resolve(string $host, string $uri): void
	{
		if (preg_match(self::HOST_PATTERN, $host) !== 1) {
			throw new TInvalidDataValueException('websocket_backplane_mesh_host_invalid', $host);
		}
		if (!isset($this->_resolving[$host])) {
			if ($this->getResolverBinary() === '') {
				$this->warnResolver('Mesh cannot look up peer hostnames: PHP runs under the ' . PHP_SAPI . ' SAPI and no PHP CLI binary was found. Set ResolverBinary to a PHP CLI binary, or give the peers by IP address.');
				throw new TConfigurationException('websocket_backplane_mesh_resolver_missing', $host);
			}
			if (count($this->_resolving) >= self::RESOLVE_QUEUE_CAP) {
				unset($this->_knownUris[$uri]);   // put off: looked up when its URI is announced or re-dialed again
				Prado::log("Mesh put off the lookup of peer host {$host}: " . self::RESOLVE_QUEUE_CAP . ' hosts are waiting for theirs', TLogger::NOTICE, static::class);
				return;
			}
			$lookup = ['process' => null, 'buffer' => '', 'deadline' => 0.0, 'uris' => []];
			if ($this->countResolvers() < self::MAX_RESOLVERS) {
				$lookup['process'] = $this->startResolver($host);
				$lookup['deadline'] = $this->getMonotonicClock()->seconds() + $this->_timeout;
			}
			$this->_resolving[$host] = $lookup;
		}
		$this->_knownUris[$uri] = true;
		$this->_resolving[$host]['uris'][$uri] = true;
	}

	/**
	 * Returns the number of lookups whose child process is running.
	 * @return int The running lookup count, at most {@see MAX_RESOLVERS}.
	 */
	private function countResolvers(): int
	{
		$count = 0;
		foreach ($this->_resolving as $lookup) {
			if ($lookup['process'] !== null) {
				$count++;
			}
		}
		return $count;
	}

	/**
	 * Starts the child process that looks up a host.  It runs the {@see getResolverBinary()
	 * ResolverBinary} without an ini file, passes the host as an argument (no shell), and writes the
	 * host's addresses to its stdout as a JSON list; its stdout is non-blocking.  A subclass may run
	 * another resolver that writes the same output.  The child is killed when its lookup passes the
	 * dial {@see getTimeout() Timeout} and at {@see close()}.
	 * @param string $host The hostname.
	 * @throws \Prado\Exceptions\TIOException When the process cannot be started.
	 * @return TProcess The running resolver.
	 * @since 1.2.0
	 */
	protected function startResolver(string $host): TProcess
	{
		$process = TProcess::open([$this->getResolverBinary(), '-n', '-r', self::RESOLVER_CODE, '--', $host], [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']]);
		$process->getStdin()?->close();
		$process->getStderr()?->close();   // the program suppresses its warnings; nothing is read here
		$stdout = $process->getStdout();
		if ($stdout !== null) {
			stream_set_blocking($stdout->getResource(), false);
		}
		return $process;
	}

	/**
	 * Finds the PHP CLI binary that runs a hostname lookup when no {@see setResolverBinary()
	 * ResolverBinary} is set: the running binary under the `cli` and `cli-server` SAPIs, else `php` in
	 * PHP's binary directory when that is an executable file.  The binary of another SAPI, such as
	 * php-fpm or php-cgi, cannot run the lookup program.
	 * @return string The binary's path, or '' when none is found.
	 * @since 1.2.0
	 */
	protected function findResolverBinary(): string
	{
		if (PHP_SAPI === 'cli' || PHP_SAPI === 'cli-server') {
			return PHP_BINARY;
		}
		$binary = PHP_BINDIR . DIRECTORY_SEPARATOR . 'php' . (DIRECTORY_SEPARATOR === '\\' ? '.exe' : '');
		return is_file($binary) && is_executable($binary) ? $binary : '';
	}

	/**
	 * Logs, once per backplane, that hostnames cannot be looked up.
	 * @param string $message The warning.
	 */
	private function warnResolver(string $message): void
	{
		if (!$this->_resolverWarned) {
			$this->_resolverWarned = true;
			Prado::log($message, TLogger::WARNING, static::class);
		}
	}

	/**
	 * Reads each running lookup's output.  A lookup that has written its addresses and exited is
	 * cached and its dials are started; one past the dial {@see getTimeout() Timeout} is killed and
	 * fails its dials, as does one that found no address.  The children killed earlier are closed once
	 * they have exited, and the lookups waiting their turn are started as running ones finish.
	 */
	private function advanceResolvers(): void
	{
		$this->reapResolvers();
		foreach (array_keys($this->_resolving) as $host) {
			$host = (string) $host;
			$lookup = $this->_resolving[$host];
			$process = $lookup['process'];
			if ($process === null) {
				continue;   // waiting its turn
			}
			$resource = $process->getStdout()?->getResource();
			$done = !is_resource($resource);
			if (!$done) {
				$chunk = @fread($resource, 4096);
				if (is_string($chunk) && $chunk !== '') {
					$lookup['buffer'] .= $chunk;
					$this->_resolving[$host]['buffer'] = $lookup['buffer'];
				}
				$done = feof($resource) || strlen($lookup['buffer']) > 65536;
			}
			if ($done) {
				unset($this->_resolving[$host]);
				$this->settleResolver($host, $process, $lookup['buffer'], $lookup['uris']);
			} elseif ($this->getMonotonicClock()->seconds() > $lookup['deadline']) {
				unset($this->_resolving[$host]);
				$this->abandonResolver($process);
				$this->failLookup($host, $lookup['uris'], 'the lookup timed out');
			}
		}
		$this->startQueuedResolvers();
	}

	/**
	 * Starts the lookups waiting their turn, in the order they were asked, while fewer than
	 * {@see MAX_RESOLVERS} run.  A lookup whose child cannot be started fails its dials.
	 */
	private function startQueuedResolvers(): void
	{
		$running = $this->countResolvers();
		foreach ($this->_resolving as $host => $lookup) {
			if ($running >= self::MAX_RESOLVERS) {
				return;
			}
			if ($lookup['process'] !== null) {
				continue;
			}
			$host = (string) $host;
			try {
				$this->_resolving[$host]['process'] = $this->startResolver($host);
			} catch (\Throwable $e) {
				unset($this->_resolving[$host]);
				$this->failLookup($host, $lookup['uris'], 'its resolver could not be started: ' . $e->getMessage());
				continue;
			}
			$this->_resolving[$host]['deadline'] = $this->getMonotonicClock()->seconds() + $this->_timeout;
			$running++;
		}
	}

	/**
	 * Caches a finished lookup's first address and starts its dials, or fails them when it found none.
	 * A resolver that wrote no address list at all did not run the lookup program, as the binary of
	 * another SAPI does not; that is logged as a warning, once.  A dial that cannot be started is
	 * forgotten, so a seed is re-dialed later and a gossiped peer when it is announced again.
	 * @param string $host The hostname.
	 * @param TProcess $process The lookup's child process.
	 * @param string $output What the child wrote.
	 * @param array<string, true> $uris The dials waiting on the lookup.
	 */
	private function settleResolver(string $host, TProcess $process, string $output, array $uris): void
	{
		$this->abandonResolver($process);
		$addresses = json_decode($output, true);
		if (!is_array($addresses)) {
			$this->warnResolver("Mesh cannot look up peer hostnames: the resolver '" . $this->getResolverBinary() . "' wrote no address list. ResolverBinary must be a PHP CLI binary.");
			$this->failLookup($host, $uris, 'its resolver wrote no answer');
			return;
		}
		$address = null;
		foreach ($addresses as $candidate) {
			if (is_string($candidate) && filter_var($candidate, FILTER_VALIDATE_IP) !== false) {
				$address = $candidate;
				break;
			}
		}
		if ($address === null) {
			$this->failLookup($host, $uris, 'it has no address');
			return;
		}
		$this->_resolved[$host] = ['address' => $address, 'expires' => $this->getMonotonicClock()->seconds() + self::RESOLVE_TTL];
		foreach (array_keys($uris) as $uri) {
			$uri = (string) $uri;
			unset($this->_knownUris[$uri]);
			try {
				$this->connectPeer($uri);
			} catch (\Throwable $e) {
				unset($this->_knownUris[$uri]);   // connectPeer() marked it known before the connect failed
				Prado::log("Mesh peer {$uri} could not be dialed: " . $e->getMessage(), TLogger::NOTICE, static::class);
			}
		}
	}

	/**
	 * Fails the dials waiting on a lookup, so a seed is re-dialed later and a gossiped peer when it
	 * is announced again.
	 * @param string $host The hostname.
	 * @param array<string, true> $uris The dials waiting on it.
	 * @param string $reason Why the lookup failed, for the log.
	 */
	private function failLookup(string $host, array $uris, string $reason): void
	{
		Prado::log("Mesh peer host {$host} could not be resolved: {$reason}", TLogger::NOTICE, static::class);
		foreach (array_keys($uris) as $uri) {
			unset($this->_knownUris[$uri]);
		}
	}

	/**
	 * Lets go of a resolver child without waiting for it.  One that has exited is closed.  One still
	 * running has its pipes closed and is killed, then closed by {@see reapResolvers()} once it has
	 * exited, since closing a running process waits for it.  The child holds no state worth a clean
	 * exit, and the kill signal cannot be caught: a signal with a handler, sent just after the fork,
	 * can run this process's handler in the child and reach this process on some platforms.
	 * @param TProcess $process The resolver.
	 */
	private function abandonResolver(TProcess $process): void
	{
		if (!$process->getIsRunning()) {
			$process->close();
			return;
		}
		foreach ($process->getPipes() as $pipe) {
			$pipe->close();
		}
		$process->terminate(self::SIGKILL);
		$this->_reaping[] = $process;
	}

	/**
	 * Closes the killed resolver children that have exited.  With a wait, it polls until every one
	 * has exited or the wait has passed, then closes the rest too, so none outlives {@see close()}.
	 * The wait is real time ({@see \hrtime()}), not the injectable monotonic clock.
	 * @param float $wait The seconds to wait for the children to exit; 0 closes only those that have.
	 */
	private function reapResolvers(float $wait = 0.0): void
	{
		$deadline = \hrtime(true) + (int) ($wait * 1_000_000_000);
		while (true) {
			foreach ($this->_reaping as $i => $process) {
				if (!$process->getIsRunning()) {
					$process->close();
					unset($this->_reaping[$i]);
				}
			}
			if ($this->_reaping === [] || \hrtime(true) >= $deadline) {
				break;
			}
			usleep(1000);
		}
		if ($wait > 0.0) {
			foreach ($this->_reaping as $process) {
				$process->close();   // killed, so its exit is due
			}
			$this->_reaping = [];
		}
	}

	/**
	 * Returns the number of hostname lookups not yet answered: those running, at most
	 * {@see MAX_RESOLVERS}, and those waiting their turn.
	 * @return int The lookup count.
	 * @since 1.2.0
	 */
	public function getResolvingCount(): int
	{
		return count($this->_resolving);
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
	 * @return string The secret, or '' when none is set (the mesh cannot open).
	 */
	public function getSecret(): string
	{
		return $this->_secret;
	}

	/**
	 * Sets the shared cluster secret a peer must prove to join.  It is required: {@see open()} refuses
	 * an empty secret, since no peer could join.  Prefer a `tls://` transport on any untrusted network.
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

	/**
	 * Returns the PHP CLI binary that runs a hostname lookup.  When none is set it is found on first
	 * use ({@see findResolverBinary()}).
	 * @return string The binary's path, or '' when none is set and none is found.
	 * @since 1.2.0
	 */
	public function getResolverBinary(): string
	{
		if ($this->_resolverBinary === null) {
			$this->_resolverBinary = $this->findResolverBinary();
		}
		return $this->_resolverBinary;
	}

	/**
	 * Sets the PHP CLI binary that runs a hostname lookup.  Set it when PHP runs under a SAPI whose
	 * binary is not the CLI, such as php-fpm, and no `php` lies in PHP's binary directory.
	 * @param string $value The path of a PHP CLI binary, or '' to find one.
	 * @return static The current backplane.
	 * @since 1.2.0
	 */
	public function setResolverBinary($value): static
	{
		$value = TPropertyValue::ensureString($value);
		$this->_resolverBinary = $value === '' ? null : $value;
		return $this;
	}

	/**
	 * Returns the `ssl` stream context options used to dial a `tls://` or `ssl://` peer.
	 * @return array<string, mixed> The options.
	 * @since 1.2.0
	 */
	public function getSslContext(): array
	{
		return $this->_sslContext;
	}

	/**
	 * Sets the `ssl` stream context options used to dial a `tls://` or `ssl://` peer, such as `cafile`
	 * for a private certificate authority.  The peer name defaults to the host in the peer's URI.  PHP's
	 * defaults verify the peer's certificate against the system store.
	 * @param array<string, mixed>|string $value The options.
	 * @return static The current backplane.
	 * @since 1.2.0
	 */
	public function setSslContext($value): static
	{
		$this->_sslContext = TPropertyValue::ensureArray($value);
		return $this;
	}

	/**
	 * Returns the maximum message size of a peer link.
	 * @return int The maximum size in bytes, or 0 for unlimited.
	 * @since 1.2.0
	 */
	public function getLinkMaxMessageSize(): int
	{
		return $this->_linkMaxMessageSize;
	}

	/**
	 * Sets the maximum message size of a peer link, applied to the links this node dials and, through
	 * {@see getEndpointMaxMessageSize()}, to those the server accepts for it.  Set the same value on
	 * every node, above the client `MaxMessageSize` by the envelope's base64 overhead (a third).
	 * Default {@see \Prado\IO\Socket\WebSocket\TWebSocketConnection::DEFAULT_MAX_MESSAGE_SIZE}.
	 * @param int|string $value The maximum size in bytes, or 0 for unlimited.
	 * @return static The current backplane.
	 * @since 1.2.0
	 */
	public function setLinkMaxMessageSize($value): static
	{
		$this->_linkMaxMessageSize = max(0, TPropertyValue::ensureInteger($value));
		return $this;
	}

	/**
	 * Returns the maximum queued outbound bytes of a peer link.
	 * @return int The maximum queued bytes, or 0 for unlimited.
	 * @since 1.2.0
	 */
	public function getLinkMaxSendBufferBytes(): int
	{
		return $this->_linkMaxSendBufferBytes;
	}

	/**
	 * Sets the maximum queued outbound bytes of a peer link, applied to dialed and accepted links.
	 * Default {@see \Prado\IO\Socket\WebSocket\TWebSocketConnection::DEFAULT_MAX_SEND_BUFFER}.
	 * @param int|string $value The maximum queued bytes, or 0 for unlimited.
	 * @return static The current backplane.
	 * @since 1.2.0
	 */
	public function setLinkMaxSendBufferBytes($value): static
	{
		$this->_linkMaxSendBufferBytes = max(0, TPropertyValue::ensureInteger($value));
		return $this;
	}

	/**
	 * Returns {@see getLinkMaxMessageSize() LinkMaxMessageSize}, the limit the server applies to a
	 * peer link it accepts for this endpoint.
	 * @return ?int The maximum size in bytes, or 0 for unlimited.
	 * @since 1.2.0
	 */
	public function getEndpointMaxMessageSize(): ?int
	{
		return $this->_linkMaxMessageSize;
	}

	/**
	 * Returns {@see getLinkMaxSendBufferBytes() LinkMaxSendBufferBytes}, the limit the server applies
	 * to a peer link it accepts for this endpoint.
	 * @return ?int The maximum queued bytes, or 0 for unlimited.
	 * @since 1.2.0
	 */
	public function getEndpointMaxSendBufferBytes(): ?int
	{
		return $this->_linkMaxSendBufferBytes;
	}

	/**
	 * Returns the pattern an announced peer URI must match to be dialed.
	 * @return string The PCRE, or '' when any announced URI is dialed.
	 * @since 1.2.0
	 */
	public function getPeerPattern(): string
	{
		return $this->_peerPattern;
	}

	/**
	 * Sets the PCRE an announced peer URI must match to be dialed, such as
	 * `#^tls://10\.0\.\d{1,3}\.\d{1,3}:8443$#D`.  Every verified peer can announce a URI that every node
	 * then dials, so a pattern keeps a node from dialing an address outside the cluster.  Anchor the
	 * pattern at both ends and match the whole host: `#^tls://10\.0\.#` also matches
	 * `tls://10.0.evil.example:443`.  The seed {@see setPeers() Peers} are always dialed.
	 * @param string $value The PCRE, or '' to dial any announced URI.
	 * @throws TInvalidDataValueException When the pattern is not a valid PCRE.
	 * @return static The current backplane.
	 * @since 1.2.0
	 */
	public function setPeerPattern($value): static
	{
		$value = TPropertyValue::ensureString($value);
		if ($value !== '' && @preg_match($value, '') === false) {
			throw new TInvalidDataValueException('websocket_backplane_mesh_peer_pattern_invalid', $value);
		}
		$this->_peerPattern = $value;
		return $this;
	}

	/**
	 * Returns the maximum number of peers accepted but not yet verified.
	 *
	 * @return int The cap, or 0 for unlimited.
	 */
	public function getMaxPendingPeers(): int
	{
		return $this->_maxPendingPeers;
	}

	/**
	 * Sets the maximum number of peers accepted but not yet verified.  A replayed handshake proof passes
	 * {@see authenticate()} repeatedly, and accepted peers do not count against the server's connection
	 * limit, so this bounds how many half-authenticated peer links a flood can hold at once.  0 removes
	 * the cap.
	 * @param int|string $value The cap, or 0 for unlimited.
	 * @return static The current backplane.
	 */
	public function setMaxPendingPeers($value): static
	{
		$this->_maxPendingPeers = max(0, (int) $value);
		return $this;
	}

	/**
	 * Returns the node-liveness TTL.
	 * @return int The TTL in seconds.
	 */
	public function getNodeTtl(): int
	{
		return $this->_nodeTtl;
	}

	/**
	 * Sets the seconds a node may go unheard before it is declared down and its clients' presence is
	 * reaped.  A heartbeat floods every third of this, so a live node is always seen within the TTL.
	 * @param int|string $value The TTL in seconds.
	 * @return static The current backplane.
	 */
	public function setNodeTtl($value): static
	{
		$this->_nodeTtl = max(1, (int) $value);
		return $this;
	}
}
