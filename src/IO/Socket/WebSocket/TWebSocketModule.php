<?php

/**
 * TWebSocketModule class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\Exceptions\TConfigurationException;
use Prado\Exceptions\TInvalidOperationException;
use Prado\IO\Socket\TSocketServer;
use Prado\IO\Socket\TSocketServerModule;
use Prado\IO\Socket\WebSocket\Cluster\IWebSocketBackplane;
use Prado\IO\Socket\WebSocket\Cluster\IWebSocketClusterAware;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\Prado;
use Prado\TComponent;
use Prado\TModule;
use Prado\TPropertyValue;
use Prado\Util\TSignalsDispatcher;
use Prado\Xml\TXmlElement;

/**
 * TWebSocketModule class.
 *
 * The WebSocket server module of the extension: a {@see TSocketServerModule} whose server is a
 * {@see TWebSocketServer}.  `php prado-cli.php websocket/serve` ({@see TWebSocketServerAction})
 * binds the configured {@see getEndpoint() Endpoint}, configures the server from the module's
 * properties, and serves until SIGTERM/SIGINT.  A web SAPI cannot host a socket server, so the
 * server runs in its own CLI process.
 *
 * The module configures the server it creates ({@see configureServer()}):
 *  - the {@see getHandlerClass() HandlerClass} handler the connections dispatch through (a
 *    {@see TWebSocketHandler} by default, whose events the module re-raises as {@see onOpen},
 *    {@see onMessage}, {@see onClose} and {@see onError}, so a configuration attribute such as
 *    `OnMessage="Application.Chat.onMessage"` receives them).  The request events of a
 *    {@see PubSub\TWebSocketPubSubHandler} ({@see onSubscribe}, {@see onPublish}, {@see onSend},
 *    {@see onCall}) are re-raised the same way, and an {@see IWebSocketClusterAware} handler is
 *    given the module's cluster;
 *  - {@see setSubprotocols() Subprotocols}, {@see setOrigins() Origins},
 *    {@see setAllowedHosts() AllowedHosts}, {@see setPermessageDeflate() PermessageDeflate};
 *  - the limits {@see setMaxMessageSize() MaxMessageSize}, {@see setMaxSendBufferBytes() MaxSendBufferBytes},
 *    {@see setHandshakeTimeout() HandshakeTimeout},
 *    {@see setIdleTimeout() IdleTimeout}, {@see setCloseTimeout() CloseTimeout} and
 *    {@see setMaxConnections() MaxConnections} (unset ones keep the server's defaults);
 *  - the {@see getCluster() cluster coordinator}, which makes the server one node of a cluster over
 *    the configured {@see Cluster\IWebSocketBackplane backplane} (a `<backplane>` child element, or
 *    the `backplane` key of an array configuration); without one the module runs a single node on a
 *    {@see TNullBackplane}.  A mesh backplane is also added as the server's peer endpoint.
 *
 * The server owns its event loop (a {@see \Prado\IO\Socket\TSocketReactor} the module supplies
 * through {@see createReactor()}), so the parent's `onClientConnect`/`onClientData`/
 * `onClientDisconnect` events are not raised; the handler events above are the connection lifecycle.
 * Application code reaches the cluster through {@see publish()}, {@see broadcast()},
 * {@see sendToClient()} and {@see presence()}.  A daemon that builds its own server hands it the
 * cluster with {@see prepareServer()}.  The {@see setNodeId() NodeId} is fixed once the cluster exists.
 *
 * The package's `websocket_*` error codes and its Prado3 short class names are registered by
 * Composer from the `extra.prado.error-messages` and `extra.prado.class-map` entries of
 * composer.json, so the extension's classes work without this module.
 *
 * ```xml
 * <modules>
 *     <module id="websockets" class="Prado\IO\Socket\WebSocket\TWebSocketModule"
 *         Address="0.0.0.0" Port="8080" HandlerClass="Application\Chat\ChatHandler"
 *         Subprotocols="chat" IdleTimeout="60" PermessageDeflate="true" NodeId="edge-1">
 *         <backplane class="Prado\IO\Socket\WebSocket\Cluster\TRedisBackplane" Host="127.0.0.1" Port="6379" />
 *     </module>
 * </modules>
 * ```
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class TWebSocketModule extends TSocketServerModule
{
	/** The default TWebSocketServer class the server is created from. */
	public const DEFAULT_SERVER_CLASS = TWebSocketServer::class;

	/** The default shell action class registered for command line control. */
	public const DEFAULT_SHELL_CLASS = TWebSocketServerAction::class;

	/** The default port the server binds to. */
	public const DEFAULT_PORT = 8080;

	/** The default handler class the connections dispatch through. */
	public const DEFAULT_HANDLER_CLASS = TWebSocketHandler::class;

	/** @var string The handler class instantiated when no handler is set. */
	private string $_handlerClass = self::DEFAULT_HANDLER_CLASS;

	/** @var ?IWebSocketHandler The handler the connections dispatch through, created on first use. */
	private ?IWebSocketHandler $_handler = null;

	/** @var string[] The subprotocols offered in preference order. */
	private array $_subprotocols = [];

	/** @var string[] The origins allowed to upgrade; empty for any. */
	private array $_origins = [];

	/** @var string[] The Host authorities allowed to upgrade; empty for any. */
	private array $_allowedHosts = [];

	/** @var bool Whether to offer RFC 7692 permessage-deflate. */
	private bool $_permessageDeflate = false;

	/** @var ?int The maximum message size, or null for the server default. */
	private ?int $_maxMessageSize = null;

	/** @var ?int The maximum queued outbound bytes per connection, or null for the server default. */
	private ?int $_maxSendBufferBytes = null;

	/** @var ?float The handshake timeout, or null for the server default. */
	private ?float $_handshakeTimeout = null;

	/** @var ?float The idle timeout, or null for the server default. */
	private ?float $_idleTimeout = null;

	/** @var ?float The close timeout, or null for the server default. */
	private ?float $_closeTimeout = null;

	/** @var ?int The maximum connections, or null for the server default. */
	private ?int $_maxConnections = null;

	/** @var ?string The configured node id; a generated id is used when null. */
	private ?string $_nodeId = null;

	/** @var ?IWebSocketBackplane The configured backplane; a {@see TNullBackplane} when null. */
	private ?IWebSocketBackplane $_backplane = null;

	/** @var ?TWebSocketCluster The cluster coordinator, created on first use. */
	private ?TWebSocketCluster $_cluster = null;

	// =========================================================================
	// Lifecycle
	// =========================================================================

	/**
	 * Initializes the module: creates the configured backplane from the `<backplane>` child element
	 * (or the `backplane` key of an array configuration), then registers the shell action when the
	 * module runs inside an application.
	 * @param null|array|TXmlElement $config The module configuration.
	 * @throws TConfigurationException When more than one backplane is configured, or a configured
	 *   backplane is missing its class or is not an {@see IWebSocketBackplane}.
	 */
	public function init($config)
	{
		if ($config instanceof TXmlElement) {
			foreach ($config->getElementsByTagName('backplane') as $element) {
				$properties = $element->getAttributes()->toArray();
				if ($this->_backplane !== null) {
					throw new TConfigurationException('websocket_backplane_multiple', (string) ($properties['class'] ?? ''));
				}
				$this->createBackplane($properties);
			}
		} elseif (is_array($config) && isset($config['backplane']) && is_array($config['backplane'])) {
			$this->createBackplane($config['backplane']);
		}
		if ($this->getApplication() === null) {
			TModule::init($config);   // standalone (a script or a test): there is no application to register the shell action on
			return;
		}
		parent::init($config);
	}

	/**
	 * Creates a backplane from a configuration map and sets it.  The class is checked before it is
	 * instantiated, so a misconfigured class name never runs a constructor.
	 * @param array<string, mixed> $properties The backplane properties, including its `class`.
	 * @throws TConfigurationException When the class is absent or not an {@see IWebSocketBackplane}.
	 */
	protected function createBackplane(array $properties): void
	{
		$class = $properties['class'] ?? null;
		unset($properties['class'], $properties['id']);
		if ($class === null || $class === '') {
			throw new TConfigurationException('websocket_backplane_class_invalid', '');
		}
		$class = (string) $class;
		if (!is_a($class, IWebSocketBackplane::class, true) || !is_a($class, TComponent::class, true)) {
			throw new TConfigurationException('websocket_backplane_class_invalid', $class);
		}
		$backplane = Prado::createComponent($class);
		foreach ($properties as $name => $value) {
			$backplane->setSubProperty($name, $value);
		}
		$this->setBackplane($backplane);
	}

	// =========================================================================
	// Server
	// =========================================================================

	/**
	 * Binds the listening {@see TWebSocketServer} and configures it from the module's properties.
	 * @throws TConfigurationException When the endpoint is unconfigured or the server class is not a TWebSocketServer.
	 * @return TWebSocketServer The configured, listening server.
	 */
	protected function createServer(): TSocketServer
	{
		$server = parent::createServer();
		if (!$server instanceof TWebSocketServer) {
			$server->close();
			throw new TConfigurationException('websocket_module_server_class_invalid', $server::class);
		}
		$this->configureServer($server);
		return $server;
	}

	/**
	 * Applies the module's handler, negotiation settings, limits, and cluster to a server.  A limit
	 * left unset keeps the server's own default.
	 * @param TWebSocketServer $server The server to configure.
	 */
	protected function configureServer(TWebSocketServer $server): void
	{
		$server->setHandler($this->getHandler());
		$server->setSubprotocols($this->_subprotocols);
		$server->setOrigins($this->_origins);
		$server->setAllowedHosts($this->_allowedHosts);
		if ($this->_permessageDeflate) {
			$server->setExtensions([new TPermessageDeflateNegotiator()]);
		}
		if ($this->_maxMessageSize !== null) {
			$server->setMaxMessageSize($this->_maxMessageSize);
		}
		if ($this->_maxSendBufferBytes !== null) {
			$server->setMaxSendBufferBytes($this->_maxSendBufferBytes);
		}
		if ($this->_handshakeTimeout !== null) {
			$server->setHandshakeTimeout($this->_handshakeTimeout);
		}
		if ($this->_idleTimeout !== null) {
			$server->setIdleTimeout($this->_idleTimeout);
		}
		if ($this->_closeTimeout !== null) {
			$server->setCloseTimeout($this->_closeTimeout);
		}
		if ($this->_maxConnections !== null) {
			$server->setMaxConnections($this->_maxConnections);
		}
		$this->prepareServer($server);
	}

	/**
	 * Drives the server's own reactor loop until {@see stop()} closes the server, delivering caught
	 * process signals between ticks.  The reactor comes from {@see createReactor()}, so a subclass
	 * can pre-register other sources or timers in the same loop.
	 * @param TSocketServer $server The listening server, a {@see TWebSocketServer}.
	 */
	protected function serveLoop(TSocketServer $server): void
	{
		if (!$server instanceof TWebSocketServer) {
			parent::serveLoop($server);
			return;
		}
		$server->setReactor($this->createReactor());
		$server->serve(1, fn () => TSignalsDispatcher::syncDispatch());   // a one-second tick keeps a sync-mode signal (and stop()) responsive
	}

	/**
	 * Hands the cluster to a server so it registers connections and pumps the backplane in its loop;
	 * a mesh backplane joins the server as its peer endpoint.
	 * @param TWebSocketServer $server The server to make a cluster node.
	 */
	public function prepareServer(TWebSocketServer $server): void
	{
		$server->setCluster($this->getCluster());
		$backplane = $this->_backplane;
		if ($backplane instanceof IWebSocketEndpoint && !in_array($backplane, $server->getEndpoints(), true)) {
			$server->addEndpoint($backplane);
		}
	}

	/**
	 * Sets the TWebSocketServer class the server is created from.
	 * @param string $value A TWebSocketServer class name.
	 * @throws TConfigurationException When the class is not a TWebSocketServer.
	 * @return static The current module.
	 */
	public function setServerClass($value): static
	{
		$value = TPropertyValue::ensureString($value);
		if (!is_a($value, TWebSocketServer::class, true)) {
			throw new TConfigurationException('websocket_module_server_class_invalid', $value);
		}
		return parent::setServerClass($value);
	}

	// =========================================================================
	// Handler and its events
	// =========================================================================

	/**
	 * Returns the handler the connections dispatch through, creating the {@see getHandlerClass()
	 * HandlerClass} on first use and binding its events to the module's.
	 * @return IWebSocketHandler The handler.
	 */
	public function getHandler(): IWebSocketHandler
	{
		if ($this->_handler === null) {
			$this->setHandler(Prado::createComponent($this->_handlerClass));
		}
		return $this->_handler;
	}

	/**
	 * Sets the handler the connections dispatch through.  A {@see TComponent} handler has its
	 * `onOpen`/`onMessage`/`onClose`/`onError` events, and the pub/sub
	 * `onSubscribe`/`onPublish`/`onSend`/`onCall` events it defines, re-raised as the module's.  An
	 * {@see IWebSocketClusterAware} handler is given the module's cluster once it exists.
	 * @param IWebSocketHandler $value The handler.
	 */
	public function setHandler(IWebSocketHandler $value): void
	{
		$this->_handler = $value;
		if ($value instanceof IWebSocketClusterAware && $this->_cluster !== null) {
			$value->setCluster($this->_cluster);
		}
		if ($value instanceof TComponent) {
			foreach (['onOpen', 'onMessage', 'onClose', 'onError', 'onSubscribe', 'onPublish', 'onSend', 'onCall'] as $event) {
				if ($value->hasEvent($event)) {
					$value->attachEventHandler($event, fn ($sender, $param) => $this->raiseEvent($event, $sender, $param));
				}
			}
		}
	}

	/**
	 * Returns the handler class instantiated when no handler is set.
	 * @return string The handler class name, default {@see DEFAULT_HANDLER_CLASS}.
	 */
	public function getHandlerClass(): string
	{
		return $this->_handlerClass;
	}

	/**
	 * Sets the handler class instantiated when no handler is set.
	 * @param string $value An {@see IWebSocketHandler} class name.
	 * @throws TConfigurationException When the class is not an IWebSocketHandler.
	 * @return static The current module.
	 */
	public function setHandlerClass($value): static
	{
		$value = TPropertyValue::ensureString($value);
		if (!is_a($value, IWebSocketHandler::class, true)) {
			throw new TConfigurationException('websocket_module_handler_class_invalid', $value);
		}
		$this->_handlerClass = $value;
		return $this;
	}

	/**
	 * Raised when a client connection opens.  The sender is the {@see TWebSocketConnection}.
	 * @param mixed $sender The connection.
	 * @param mixed $param Null.
	 */
	public function onOpen(mixed $sender, mixed $param = null): void
	{
		$this->raiseEvent('onOpen', $sender, $param);
	}

	/**
	 * Raised with each complete message.  The sender is the {@see TWebSocketConnection}.
	 * @param mixed $sender The connection.
	 * @param mixed $param The {@see TWebSocketMessage}.
	 */
	public function onMessage(mixed $sender, mixed $param = null): void
	{
		$this->raiseEvent('onMessage', $sender, $param);
	}

	/**
	 * Raised when a client connection closes.  The sender is the {@see TWebSocketConnection}.
	 * @param mixed $sender The connection.
	 * @param mixed $param Null.
	 */
	public function onClose(mixed $sender, mixed $param = null): void
	{
		$this->raiseEvent('onClose', $sender, $param);
	}

	/**
	 * Raised when a connection fails with a protocol error.  The sender is the {@see TWebSocketConnection}.
	 * @param mixed $sender The connection.
	 * @param mixed $param The {@see \Throwable}.
	 */
	public function onError(mixed $sender, mixed $param = null): void
	{
		$this->raiseEvent('onError', $sender, $param);
	}

	/**
	 * Raised before a pub/sub client joins a channel.  The sender is the {@see TWebSocketConnection}.
	 * @param mixed $sender The connection.
	 * @param mixed $param The {@see PubSub\TWebSocketPubSubEventParameter}.
	 * @since 1.2.0
	 */
	public function onSubscribe(mixed $sender, mixed $param = null): void
	{
		$this->raiseEvent('onSubscribe', $sender, $param);
	}

	/**
	 * Raised before a pub/sub client publishes.  The sender is the {@see TWebSocketConnection}.
	 * @param mixed $sender The connection.
	 * @param mixed $param The {@see PubSub\TWebSocketPubSubEventParameter}.
	 * @since 1.2.0
	 */
	public function onPublish(mixed $sender, mixed $param = null): void
	{
		$this->raiseEvent('onPublish', $sender, $param);
	}

	/**
	 * Raised before a pub/sub client sends a direct message.  The sender is the {@see TWebSocketConnection}.
	 * @param mixed $sender The connection.
	 * @param mixed $param The {@see PubSub\TWebSocketPubSubEventParameter}.
	 * @since 1.2.0
	 */
	public function onSend(mixed $sender, mixed $param = null): void
	{
		$this->raiseEvent('onSend', $sender, $param);
	}

	/**
	 * Raised for a pub/sub client call.  The sender is the {@see TWebSocketConnection}.
	 * @param mixed $sender The connection.
	 * @param mixed $param The {@see PubSub\TWebSocketPubSubEventParameter}.
	 * @since 1.2.0
	 */
	public function onCall(mixed $sender, mixed $param = null): void
	{
		$this->raiseEvent('onCall', $sender, $param);
	}

	// =========================================================================
	// Negotiation and limits
	// =========================================================================

	/**
	 * Returns the subprotocols offered in preference order.
	 * @return string[] The subprotocols.
	 */
	public function getSubprotocols(): array
	{
		return $this->_subprotocols;
	}

	/**
	 * Sets the subprotocols offered in preference order.
	 * @param array|string $value The subprotocols, or a comma-separated list.
	 * @return static The current module.
	 */
	public function setSubprotocols($value): static
	{
		$this->_subprotocols = TPropertyValue::ensureArray($value);
		return $this;
	}

	/**
	 * Returns the origins allowed to upgrade.
	 * @return string[] The origins; empty for any.
	 */
	public function getOrigins(): array
	{
		return $this->_origins;
	}

	/**
	 * Sets the origins allowed to upgrade.
	 * @param array|string $value The origins, or a comma-separated list; empty for any.
	 * @return static The current module.
	 */
	public function setOrigins($value): static
	{
		$this->_origins = TPropertyValue::ensureArray($value);
		return $this;
	}

	/**
	 * Returns the Host authorities allowed to upgrade.
	 * @return string[] The hosts; empty for any.
	 */
	public function getAllowedHosts(): array
	{
		return $this->_allowedHosts;
	}

	/**
	 * Sets the Host authorities allowed to upgrade.
	 * @param array|string $value The hosts, or a comma-separated list; empty for any.
	 * @return static The current module.
	 */
	public function setAllowedHosts($value): static
	{
		$this->_allowedHosts = TPropertyValue::ensureArray($value);
		return $this;
	}

	/**
	 * Returns whether RFC 7692 permessage-deflate is offered.
	 * @return bool Whether the extension is offered.
	 */
	public function getPermessageDeflate(): bool
	{
		return $this->_permessageDeflate;
	}

	/**
	 * Sets whether to offer RFC 7692 permessage-deflate (needs ext-zlib).
	 * @param bool|string $value Whether to offer the extension.
	 * @return static The current module.
	 */
	public function setPermessageDeflate($value): static
	{
		$this->_permessageDeflate = TPropertyValue::ensureBoolean($value);
		return $this;
	}

	/**
	 * Returns the maximum message size applied to the server.
	 * @return ?int The size in bytes, or null for the server default.
	 */
	public function getMaxMessageSize(): ?int
	{
		return $this->_maxMessageSize;
	}

	/**
	 * Sets the maximum message size applied to the server ({@see TWebSocketServer::setMaxMessageSize()}).
	 * @param null|int|string $value The size in bytes, 0 for unlimited, or null for the server default.
	 * @return static The current module.
	 */
	public function setMaxMessageSize($value): static
	{
		$this->_maxMessageSize = ($value === null || $value === '') ? null : TPropertyValue::ensureInteger($value);
		return $this;
	}

	/**
	 * Returns the maximum queued outbound bytes per connection applied to the server.
	 * @return ?int The size in bytes, or null for the server default.
	 */
	public function getMaxSendBufferBytes(): ?int
	{
		return $this->_maxSendBufferBytes;
	}

	/**
	 * Sets the maximum queued outbound bytes per connection applied to the server
	 * ({@see TWebSocketServer::setMaxSendBufferBytes()}).
	 * @param null|int|string $value The size in bytes, 0 for unlimited, or null for the server default.
	 * @return static The current module.
	 */
	public function setMaxSendBufferBytes($value): static
	{
		$this->_maxSendBufferBytes = ($value === null || $value === '') ? null : TPropertyValue::ensureInteger($value);
		return $this;
	}

	/**
	 * Returns the handshake timeout applied to the server.
	 * @return ?float The seconds, or null for the server default.
	 */
	public function getHandshakeTimeout(): ?float
	{
		return $this->_handshakeTimeout;
	}

	/**
	 * Sets the handshake timeout applied to the server ({@see TWebSocketServer::setHandshakeTimeout()}).
	 * @param null|float|int|string $value The seconds, or null for the server default.
	 * @return static The current module.
	 */
	public function setHandshakeTimeout($value): static
	{
		$this->_handshakeTimeout = ($value === null || $value === '') ? null : TPropertyValue::ensureFloat($value);
		return $this;
	}

	/**
	 * Returns the idle timeout applied to the server.
	 * @return ?float The seconds, or null for the server default.
	 */
	public function getIdleTimeout(): ?float
	{
		return $this->_idleTimeout;
	}

	/**
	 * Sets the idle timeout applied to the server ({@see TWebSocketServer::setIdleTimeout()}).
	 * @param null|float|int|string $value The seconds, 0 to disable, or null for the server default.
	 * @return static The current module.
	 */
	public function setIdleTimeout($value): static
	{
		$this->_idleTimeout = ($value === null || $value === '') ? null : TPropertyValue::ensureFloat($value);
		return $this;
	}

	/**
	 * Returns the close timeout applied to the server.
	 * @return ?float The seconds, or null for the server default.
	 */
	public function getCloseTimeout(): ?float
	{
		return $this->_closeTimeout;
	}

	/**
	 * Sets the close timeout applied to the server ({@see TWebSocketServer::setCloseTimeout()}).
	 * @param null|float|int|string $value The seconds, or null for the server default.
	 * @return static The current module.
	 */
	public function setCloseTimeout($value): static
	{
		$this->_closeTimeout = ($value === null || $value === '') ? null : TPropertyValue::ensureFloat($value);
		return $this;
	}

	/**
	 * Returns the connection cap applied to the server.
	 * @return ?int The cap, or null for the server default.
	 */
	public function getMaxConnections(): ?int
	{
		return $this->_maxConnections;
	}

	/**
	 * Sets the connection cap applied to the server ({@see TWebSocketServer::setMaxConnections()}).
	 * @param null|int|string $value The cap, 0 for unlimited, or null for the server default.
	 * @return static The current module.
	 */
	public function setMaxConnections($value): static
	{
		$this->_maxConnections = ($value === null || $value === '') ? null : TPropertyValue::ensureInteger($value);
		return $this;
	}

	// =========================================================================
	// Cluster
	// =========================================================================

	/**
	 * Returns the cluster coordinator, creating it from the configured node id and backplane on
	 * first use.  A handler already set that is {@see IWebSocketClusterAware} is given the new cluster.
	 * @return TWebSocketCluster The cluster coordinator.
	 */
	public function getCluster(): TWebSocketCluster
	{
		if ($this->_cluster === null) {
			$this->_cluster = new TWebSocketCluster($this->_nodeId, $this->_backplane ?? new TNullBackplane());
			if ($this->_handler instanceof IWebSocketClusterAware) {
				$this->_handler->setCluster($this->_cluster);
			}
		}
		return $this->_cluster;
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
	 * Sets the node id identifying this server in the cluster.  The id is fixed once the cluster is
	 * created by {@see getCluster()}, since the cluster and its backplane announce it.
	 * @param ?string $value The node id; null or '' selects a generated id.
	 * @throws TInvalidOperationException When the cluster already exists.
	 */
	public function setNodeId(?string $value): void
	{
		if ($this->_cluster !== null) {
			throw new TInvalidOperationException('websocket_module_node_id_immutable');
		}
		$this->_nodeId = ($value === null || $value === '') ? null : $value;
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
	 * @param bool $binary Whether to deliver the payload as a Binary message. Default false (Text).
	 */
	public function publish(string $channel, string $payload, bool $binary = false): void
	{
		$this->getCluster()->publish($channel, $payload, $binary);
	}

	/**
	 * Broadcasts a payload to every client in the cluster.
	 * @param string $payload The message payload.
	 * @param bool $binary Whether to deliver the payload as a Binary message. Default false (Text).
	 */
	public function broadcast(string $payload, bool $binary = false): void
	{
		$this->getCluster()->broadcast($payload, $binary);
	}

	/**
	 * Sends a payload to one client wherever it is connected in the cluster.
	 * @param string $clientId The cluster client id.
	 * @param string $payload The message payload.
	 * @param bool $binary Whether to deliver the payload as a Binary message. Default false (Text).
	 * @return bool Whether the client is known.
	 */
	public function sendToClient(string $clientId, string $payload, bool $binary = false): bool
	{
		return $this->getCluster()->sendToClient($clientId, $payload, $binary);
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
