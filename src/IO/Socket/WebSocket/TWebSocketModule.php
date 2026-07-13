<?php

/**
 * TWebSocketModule class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\Exceptions\TConfigurationException;
use Prado\IO\Socket\WebSocket\Cluster\IWebSocketBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\Prado;
use Prado\TComponent;
use Prado\TModule;
use Prado\Xml\TXmlElement;

/**
 * TWebSocketModule class.
 *
 * The cluster module of the PRADO WebSockets extension.  The package's `websocket_*` error codes and
 * its Prado3 short class names are registered by Composer from the `extra.prado.error-messages` and
 * `extra.prado.class-map` entries of composer.json, so the extension's default features work without
 * this module; configure it only to run the server as a cluster node.
 *
 * The module owns the {@see TWebSocketCluster cluster coordinator}, which makes the server one
 * node of a cluster.  Configure a {@see Cluster\IWebSocketBackplane backplane} as a child element to
 * relay across nodes; without one the module runs a single node on a {@see TNullBackplane}.  A
 * daemon hands the cluster to its server with {@see prepareServer()}, and application code reaches
 * the cluster through {@see publish()}, {@see broadcast()}, {@see sendToClient()}, and
 * {@see presence()}.
 *
 * Configure the WebSocket service alongside it, routed by
 * {@see \Prado\Web\Behaviors\TRequestConnectionUpgrade}:
 *
 * ```xml
 * <modules>
 *     <module id="websockets" class="Prado\IO\Socket\WebSocket\TWebSocketModule" NodeId="edge-1">
 *         <backplane class="Prado\IO\Socket\WebSocket\Cluster\TRedisBackplane" Host="127.0.0.1" Port="6379" />
 *     </module>
 * </modules>
 * <services>
 *     <service id="websocket" class="Prado\Web\Services\TWebSocketService" />
 * </services>
 * ```
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.0.0
 */
class TWebSocketModule extends TModule
{
	/** @var ?string The configured node id; a generated id is used when null. */
	private ?string $_nodeId = null;

	/** @var ?IWebSocketBackplane The configured backplane; a {@see TNullBackplane} when null. */
	private ?IWebSocketBackplane $_backplane = null;

	/** @var ?TWebSocketCluster The cluster coordinator, created on first use. */
	private ?TWebSocketCluster $_cluster = null;

	/**
	 * Initializes the module, creating the configured backplane from a `<backplane>` child element.
	 * @param null|array|TXmlElement $config The module configuration.
	 * @throws TConfigurationException When a configured backplane is missing or invalid.
	 */
	public function init($config)
	{
		if ($config instanceof TXmlElement) {
			foreach ($config->getElementsByTagName('backplane') as $element) {
				$this->createBackplane($element->getAttributes()->toArray());
			}
		} elseif (is_array($config) && isset($config['backplane']) && is_array($config['backplane'])) {
			$this->createBackplane($config['backplane']);
		}
		parent::init($config);
	}

	/**
	 * Creates a backplane from a configuration map and sets it.
	 * @param array<string, mixed> $properties The backplane properties, including its `class`.
	 * @throws TConfigurationException When the class is absent or not an {@see IWebSocketBackplane}.
	 */
	protected function createBackplane(array $properties): void
	{
		$class = $properties['class'] ?? null;
		unset($properties['class'], $properties['id']);
		if ($class === null) {
			throw new TConfigurationException('websocket_backplane_class_invalid', '');
		}
		$backplane = Prado::createComponent((string) $class);
		if (!($backplane instanceof IWebSocketBackplane) || !($backplane instanceof TComponent)) {
			throw new TConfigurationException('websocket_backplane_class_invalid', (string) $class);
		}
		foreach ($properties as $name => $value) {
			$backplane->setSubProperty($name, $value);
		}
		$this->setBackplane($backplane);
	}

	/**
	 * Returns the cluster coordinator, creating it from the configured node id and backplane on
	 * first use.
	 * @return TWebSocketCluster The cluster coordinator.
	 */
	public function getCluster(): TWebSocketCluster
	{
		if ($this->_cluster === null) {
			$this->_cluster = new TWebSocketCluster($this->_nodeId, $this->_backplane ?? new TNullBackplane());
		}
		return $this->_cluster;
	}

	/**
	 * Hands the cluster to a server so it registers connections and pumps the backplane in its loop.
	 * @param TWebSocketServer $server The server to make a cluster node.
	 */
	public function prepareServer(TWebSocketServer $server): void
	{
		$server->setCluster($this->getCluster());
	}

	/**
	 * Returns the configured node id.
	 * @return ?string The node id, or null when generated.
	 */
	public function getNodeId(): ?string
	{
		return $this->_nodeId;
	}

	/**
	 * Sets the node id identifying this server in the cluster.
	 * @param ?string $value The node id.
	 */
	public function setNodeId($value): void
	{
		$this->_nodeId = ($value === null || $value === '') ? null : (string) $value;
	}

	/**
	 * Returns the backplane.
	 * @return ?IWebSocketBackplane The backplane, or null when none is configured.
	 */
	public function getBackplane(): ?IWebSocketBackplane
	{
		return $this->_backplane;
	}

	/**
	 * Sets the backplane, applying it to the cluster when one is already created.
	 * @param IWebSocketBackplane $value The backplane.
	 */
	public function setBackplane(IWebSocketBackplane $value): void
	{
		$this->_backplane = $value;
		$this->_cluster?->setBackplane($value);
	}

	/**
	 * Publishes a payload to the subscribers of a channel across the cluster.
	 * @param string $channel The channel name.
	 * @param string $payload The message payload.
	 */
	public function publish(string $channel, string $payload): void
	{
		$this->getCluster()->publish($channel, $payload);
	}

	/**
	 * Broadcasts a payload to every client in the cluster.
	 * @param string $payload The message payload.
	 */
	public function broadcast(string $payload): void
	{
		$this->getCluster()->broadcast($payload);
	}

	/**
	 * Sends a payload to one client wherever it is connected in the cluster.
	 * @param string $clientId The cluster client id.
	 * @param string $payload The message payload.
	 * @return bool Whether the client is known.
	 */
	public function sendToClient(string $clientId, string $payload): bool
	{
		return $this->getCluster()->sendToClient($clientId, $payload);
	}

	/**
	 * Returns the cluster-wide presence mirror.
	 * @return array<string, array<string, mixed>> The presence metadata, keyed by client id.
	 */
	public function presence(): array
	{
		return $this->getCluster()->presence();
	}
}
