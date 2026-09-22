<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TConfigurationException;
use Prado\Exceptions\TInvalidOperationException;
use Prado\IO\Socket\WebSocket\Cluster\TFileBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketModule;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\IO\Stream\TBufferStream;
use Prado\TComponent;
use Prado\Xml\TXmlElement;

/** Exposes the protected backplane factory for direct testing. */
class TestableWebSocketModule extends TWebSocketModule
{
	public function createBackplanePublic(array $properties): void
	{
		$this->createBackplane($properties);
	}
}

/** A component that is not a backplane and records whether it was ever constructed. */
class ConstructionCountingComponent extends TComponent
{
	public static int $constructed = 0;

	public function __construct()
	{
		self::$constructed++;
		parent::__construct();
	}
}

class TWebSocketModuleClusterTest extends TestCase
{
	private array $tempDirs = [];

	protected function tearDown(): void
	{
		foreach ($this->tempDirs as $dir) {
			foreach (glob($dir . DIRECTORY_SEPARATOR . '*') ?: [] as $f) {
				is_dir($f) ? @rmdir($f) : @unlink($f);
			}
			foreach (glob($dir . DIRECTORY_SEPARATOR . '*' . DIRECTORY_SEPARATOR . '*') ?: [] as $f) {
				@unlink($f);
			}
			@rmdir($dir . DIRECTORY_SEPARATOR . TFileBackplane::PRESENCE_DIR);
			@rmdir($dir);
		}
	}

	private function tempDir(): string
	{
		$dir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'wsmod_' . uniqid('', true);
		$this->tempDirs[] = $dir;
		return $dir;
	}

	/**
	 * Builds a `<module>` element with the given `<backplane>` children.
	 * @param array<int, array<string, string>> $backplanes The attribute maps, one per backplane element.
	 * @return TXmlElement The module configuration.
	 */
	private function xmlConfig(array $backplanes): TXmlElement
	{
		$config = new TXmlElement('module');
		foreach ($backplanes as $attributes) {
			$element = new TXmlElement('backplane');
			foreach ($attributes as $name => $value) {
				$element->setAttribute($name, $value);
			}
			$config->getElements()->add($element);
		}
		return $config;
	}

	/**
	 * Decodes the unmasked server frames a connection wrote to its buffer stream.
	 * @param TBufferStream $stream The connection's transport.
	 * @return array<int, array{0: int, 1: string}> The opcode and payload of each frame.
	 */
	private function framesOn(TBufferStream $stream): array
	{
		$wire = (string) $stream;
		$frames = [];
		while (($decoded = TWebSocketFrameCodec::tryDecode($wire, false)) !== null) {
			$frames[] = [$decoded['frame']->getOpcode(), $decoded['frame']->getPayload()];
			$wire = substr($wire, $decoded['length']);
		}
		return $frames;
	}

	public function testClusterUsesConfiguredNodeIdAndBackplane()
	{
		$module = new TWebSocketModule();
		$module->setNodeId('edge-1');
		$backplane = new TNullBackplane();
		$module->setBackplane($backplane);

		$cluster = $module->getCluster();
		self::assertSame('edge-1', $cluster->getNodeId());
		self::assertSame($backplane, $cluster->getBackplane());
		self::assertSame($cluster, $module->getCluster(), 'The cluster is created once.');
	}

	public function testNodeIdIsTypedAndFixedOnceTheClusterExists()
	{
		$module = new TWebSocketModule();
		$module->setNodeId('');
		self::assertNull($module->getNodeId(), 'An empty id selects a generated id.');
		$module->setNodeId('edge-2');
		self::assertSame('edge-2', $module->getNodeId());
		$module->getCluster();
		try {
			$module->setNodeId('edge-3');
			self::fail('The node id cannot change after the cluster announced it.');
		} catch (TInvalidOperationException $e) {
			self::assertSame('websocket_module_node_id_immutable', $e->getErrorCode());
		}
		self::assertSame('edge-2', $module->getNodeId(), 'The rejected id is not applied.');
	}

	public function testSetBackplaneAfterClusterCreationUpdatesIt()
	{
		$module = new TWebSocketModule();
		$cluster = $module->getCluster();   // creates with the default null backplane
		$backplane = new TNullBackplane();
		$module->setBackplane($backplane);
		self::assertSame($backplane, $cluster->getBackplane(), 'A later backplane is applied to the live cluster.');
	}

	public function testPrepareServerWiresTheClusterIntoTheServer()
	{
		$module = new TWebSocketModule();
		$server = new TWebSocketServer();
		$module->prepareServer($server);
		self::assertSame($module->getCluster(), $server->getCluster());
	}

	public function testCreateBackplaneInstantiatesAndConfigures()
	{
		$dir = $this->tempDir();
		$module = new TestableWebSocketModule();
		$module->createBackplanePublic(['class' => TFileBackplane::class, 'Directory' => $dir]);

		$backplane = $module->getBackplane();
		self::assertInstanceOf(TFileBackplane::class, $backplane);
		self::assertSame($dir, $backplane->getDirectory(), 'Configured attributes are applied to the backplane.');
	}

	public function testCreateBackplaneRejectsAnInvalidClassWithoutInstantiatingIt()
	{
		$module = new TestableWebSocketModule();
		ConstructionCountingComponent::$constructed = 0;
		try {
			$module->createBackplanePublic(['class' => ConstructionCountingComponent::class]);
			self::fail('A component that is not a backplane is refused.');
		} catch (TConfigurationException $e) {
			self::assertSame('websocket_backplane_class_invalid', $e->getErrorCode());
		}
		self::assertSame(0, ConstructionCountingComponent::$constructed, 'The class is checked before it is constructed.');
		self::assertNull($module->getBackplane());

		try {
			$module->createBackplanePublic(['class' => \stdClass::class]);
			self::fail('A non-component is refused.');
		} catch (TConfigurationException $e) {
			self::assertSame('websocket_backplane_class_invalid', $e->getErrorCode());
		}
		try {
			$module->createBackplanePublic(['Directory' => '/tmp']);
			self::fail('A backplane without a class is refused.');
		} catch (TConfigurationException $e) {
			self::assertSame('websocket_backplane_class_invalid', $e->getErrorCode());
		}
	}

	public function testInitCreatesTheBackplaneFromXmlConfiguration()
	{
		$dir = $this->tempDir();
		$module = new TWebSocketModule();
		$module->init($this->xmlConfig([['class' => TFileBackplane::class, 'Directory' => $dir, 'id' => 'ignored']]));

		$backplane = $module->getBackplane();
		self::assertInstanceOf(TFileBackplane::class, $backplane);
		self::assertSame($dir, $backplane->getDirectory());
		self::assertSame($backplane, $module->getCluster()->getBackplane(), 'The configured backplane drives the cluster.');
	}

	public function testInitCreatesTheBackplaneFromArrayConfiguration()
	{
		$dir = $this->tempDir();
		$module = new TWebSocketModule();
		$module->init(['backplane' => ['class' => TFileBackplane::class, 'Directory' => $dir]]);
		self::assertInstanceOf(TFileBackplane::class, $module->getBackplane());
		self::assertSame($dir, $module->getBackplane()->getDirectory());

		$plain = new TWebSocketModule();
		$plain->init(['backplane' => 'not-a-map']);
		self::assertNull($plain->getBackplane(), 'A backplane entry that is not a map is ignored.');
		$plain->init(null);
		self::assertNull($plain->getBackplane(), 'No configuration means a single node on the null backplane.');
		self::assertInstanceOf(TNullBackplane::class, $plain->getCluster()->getBackplane());
	}

	public function testInitRejectsASecondBackplaneElement()
	{
		$module = new TWebSocketModule();
		try {
			$module->init($this->xmlConfig([
				['class' => TNullBackplane::class],
				['class' => TFileBackplane::class, 'Directory' => $this->tempDir()],
			]));
			self::fail('Two backplanes cannot both relay for one node.');
		} catch (TConfigurationException $e) {
			self::assertSame('websocket_backplane_multiple', $e->getErrorCode());
			self::assertStringContainsString(TFileBackplane::class, $e->getMessage(), 'The message names the surplus backplane.');
		}
		self::assertInstanceOf(TNullBackplane::class, $module->getBackplane(), 'The first backplane stands; the second is refused.');
	}

	public function testConvenienceApiPublishesThroughTheCluster()
	{
		$dir = $this->tempDir();
		$module = new TWebSocketModule();
		$module->setNodeId('n1');
		$backplane = new TFileBackplane();
		$backplane->setDirectory($dir);
		$module->setBackplane($backplane);

		$cluster = $module->getCluster();
		$cluster->open();
		$stream = new TBufferStream();
		$connection = new TWebSocketConnection($stream, false);
		$id = $cluster->register($connection);
		$cluster->subscribe($id, 'news');

		$module->publish('news', 'hello');
		self::assertGreaterThan(0, $stream->getSize(), 'Module publish reaches a local subscriber through the cluster.');

		$cluster->close();
	}

	public function testBroadcastSendToClientAndPresenceGoThroughTheCluster()
	{
		$module = new TWebSocketModule();
		$module->setNodeId('n1');
		$module->setBackplane(new TNullBackplane());
		$cluster = $module->getCluster();
		$cluster->open();

		$first = new TBufferStream();
		$second = new TBufferStream();
		$firstId = $cluster->register(new TWebSocketConnection($first, false), ['user' => 'ann']);
		$secondId = $cluster->register(new TWebSocketConnection($second, false));

		$presence = $module->presence();
		self::assertSame([$firstId, $secondId], array_keys($presence), 'presence() mirrors every registered client.');
		self::assertSame('ann', $presence[$firstId]['user']);
		self::assertSame('n1', $presence[$firstId]['node']);

		$module->broadcast('all');
		self::assertSame([[TWebSocketOpcode::Text, 'all']], $this->framesOn($first), 'broadcast() reaches every client.');
		self::assertSame([[TWebSocketOpcode::Text, 'all']], $this->framesOn($second));

		self::assertTrue($module->sendToClient($secondId, "\x00\x01", true), 'sendToClient() addresses one known client.');
		self::assertSame([[TWebSocketOpcode::Text, 'all']], $this->framesOn($first), 'The other client receives nothing.');
		self::assertSame([[TWebSocketOpcode::Text, 'all'], [TWebSocketOpcode::Binary, "\x00\x01"]], $this->framesOn($second), 'A binary send arrives as a Binary frame.');
		self::assertFalse($module->sendToClient('nobody', 'x'), 'An unknown client is reported.');

		$cluster->close();
	}
}
