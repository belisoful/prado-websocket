<?php

use Prado\IO\Http2\TH2Session;
use Prado\IO\Http2\TNgHttp2;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\THttp2WebSocketProtocol;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;

/**
 * Verifies that WebSocket-over-HTTP/2 streams join and leave the cluster per stream, using the
 * protocol's {@see THttp2WebSocketProtocol::onConnection}/{@see THttp2WebSocketProtocol::onClose}
 * events the server wires to the cluster.  Skipped when libnghttp2 is unavailable.
 */
class TWebSocketHttp2ClusterTest extends PHPUnit\Framework\TestCase
{
	protected function setUp(): void
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
	}

	public function testHttp2StreamRegistersAndUnregistersWithTheCluster()
	{
		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$cluster = new TWebSocketCluster('h2', new TNullBackplane());
		$protocol->attachEventHandler('onConnection', fn ($sender, $connection) => $cluster->register($connection));
		$protocol->attachEventHandler('onClose', fn ($sender, $connection) => $cluster->unregister($connection));

		$client = new TH2Session(false);
		$client->submitSettings([]);
		$stream = $client->request([
			':method' => 'CONNECT',
			':protocol' => 'websocket',
			':scheme' => 'https',
			':path' => '/chat',
			':authority' => 'example.com',
			'sec-websocket-version' => '13',
		]);

		$protocol->receive($client->send());   // Extended CONNECT -> accept stream -> register
		self::assertCount(1, $cluster->presence(), 'An HTTP/2 WebSocket stream registers with the cluster.');
		self::assertCount(1, $protocol->getConnections());

		$client->resetStream($stream->getStreamId());
		$protocol->receive($client->send());   // RST_STREAM -> close stream -> unregister
		self::assertCount(0, $cluster->presence(), 'A closed HTTP/2 stream unregisters from the cluster.');
		self::assertCount(0, $protocol->getConnections());

		$protocol->getSession()->close();
		$client->close();
	}
}
