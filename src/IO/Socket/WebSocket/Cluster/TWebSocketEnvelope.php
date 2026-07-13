<?php

/**
 * TWebSocketEnvelope class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

use Prado\TComponent;

/**
 * TWebSocketEnvelope class.
 *
 * The unit of exchange between cluster nodes.  A {@see TWebSocketCluster} coordinator hands an
 * envelope to its {@see IWebSocketBackplane} to fan out, and the backplane delivers received
 * envelopes back to the coordinator, which routes them to local clients.
 *
 * The {@see getType() Type} selects the routing model:
 *  - {@see PUBLISH}: deliver {@see getPayload() Payload} to local subscribers of {@see getChannel() Channel}.
 *  - {@see BROADCAST}: deliver Payload to every local client.
 *  - {@see DIRECT}: deliver Payload to the local client named by {@see getClientId() ClientId}.
 *  - {@see PRESENCE_SET}/{@see PRESENCE_DROP}: update the presence mirror for ClientId.
 *  - {@see NODE_UP}/{@see NODE_DOWN}: update the node registry mirror for {@see getOriginNode() OriginNode}.
 *
 * Each envelope carries OriginNode and a unique {@see getId() Id} so a coordinator drops its own
 * echo and a mesh backplane can suppress duplicate relays.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 4.4.0
 */
class TWebSocketEnvelope extends TComponent
{
	/** Deliver to subscribers of a channel. */
	public const PUBLISH = 'publish';

	/** Deliver to every client. */
	public const BROADCAST = 'broadcast';

	/** Deliver to one client by its cluster id. */
	public const DIRECT = 'direct';

	/** A client became present on a node. */
	public const PRESENCE_SET = 'presence.set';

	/** A client left a node. */
	public const PRESENCE_DROP = 'presence.drop';

	/** A node joined the cluster. */
	public const NODE_UP = 'node.up';

	/** A node left the cluster. */
	public const NODE_DOWN = 'node.down';

	/** @var string The routing type, one of the class constants. */
	private string $_type;

	/** @var string The id of the node the envelope originated on. */
	private string $_originNode;

	/** @var string The message payload. */
	private string $_payload;

	/** @var ?string The target channel for a {@see PUBLISH}. */
	private ?string $_channel;

	/** @var ?string The target client for a {@see DIRECT} or the subject of a presence change. */
	private ?string $_clientId;

	/** @var array<string, mixed> Out-of-band attributes (presence metadata, node info). */
	private array $_meta;

	/** @var string A unique id for echo and duplicate suppression. */
	private string $_id;

	/**
	 * @param string $type The routing type, one of the class constants.
	 * @param string $originNode The originating node id.
	 * @param string $payload The message payload.
	 * @param ?string $channel The target channel for a {@see PUBLISH}.
	 * @param ?string $clientId The target/subject client id.
	 * @param array<string, mixed> $meta Out-of-band attributes.
	 * @param string $id A unique id; an empty value is replaced with a generated one.
	 */
	public function __construct(string $type, string $originNode, string $payload = '', ?string $channel = null, ?string $clientId = null, array $meta = [], string $id = '')
	{
		$this->_type = $type;
		$this->_originNode = $originNode;
		$this->_payload = $payload;
		$this->_channel = $channel;
		$this->_clientId = $clientId;
		$this->_meta = $meta;
		$this->_id = $id !== '' ? $id : ($originNode . '-' . bin2hex(random_bytes(8)));
		parent::__construct();
	}

	/**
	 * Returns the routing type.
	 * @return string The routing type, one of the class constants.
	 */
	public function getType(): string
	{
		return $this->_type;
	}

	/**
	 * Returns the originating node id.
	 * @return string The node the envelope originated on.
	 */
	public function getOriginNode(): string
	{
		return $this->_originNode;
	}

	/**
	 * Returns the message payload.
	 * @return string The payload.
	 */
	public function getPayload(): string
	{
		return $this->_payload;
	}

	/**
	 * Returns the target channel.
	 * @return ?string The channel for a {@see PUBLISH}, or null.
	 */
	public function getChannel(): ?string
	{
		return $this->_channel;
	}

	/**
	 * Returns the target or subject client id.
	 * @return ?string The client id, or null.
	 */
	public function getClientId(): ?string
	{
		return $this->_clientId;
	}

	/**
	 * Returns the out-of-band attributes.
	 * @return array<string, mixed> The metadata.
	 */
	public function getMeta(): array
	{
		return $this->_meta;
	}

	/**
	 * Returns the unique envelope id.
	 * @return string The id.
	 */
	public function getId(): string
	{
		return $this->_id;
	}

	/**
	 * Serializes the envelope to a JSON string for transport.
	 * @return string The encoded envelope.
	 */
	public function encode(): string
	{
		return json_encode([
			't' => $this->_type,
			'o' => $this->_originNode,
			'p' => $this->_payload,
			'c' => $this->_channel,
			'k' => $this->_clientId,
			'm' => $this->_meta,
			'i' => $this->_id,
		], JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
	}

	/**
	 * Reconstructs an envelope from its {@see encode() encoded} form.
	 * @param string $json The encoded envelope.
	 * @return ?self The envelope, or null when the input is malformed.
	 */
	public static function decode(string $json): ?self
	{
		$data = json_decode($json, true);
		if (!is_array($data) || !isset($data['t'], $data['o']) || !is_scalar($data['t']) || !is_scalar($data['o'])) {
			return null;
		}
		foreach (['p', 'c', 'k', 'i'] as $field) {
			if (isset($data[$field]) && !is_scalar($data[$field])) {
				return null;   // a non-scalar field would fatal on the string cast; reject the forged envelope instead
			}
		}
		return new self(
			(string) $data['t'],
			(string) $data['o'],
			(string) ($data['p'] ?? ''),
			isset($data['c']) ? (string) $data['c'] : null,
			isset($data['k']) ? (string) $data['k'] : null,
			is_array($data['m'] ?? null) ? $data['m'] : [],
			(string) ($data['i'] ?? ''),
		);
	}
}
