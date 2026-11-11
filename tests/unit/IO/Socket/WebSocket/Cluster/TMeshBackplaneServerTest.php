<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\IO\Stream\TBufferStream;

/**
 * Mesh dials against a real {@see TWebSocketServer} serving the mesh endpoint: the dial's connect,
 * TLS handshake, and WebSocket upgrade advance without blocking either side's loop, and the
 * certificate of a `tls://` peer is verified against the host in its URI.
 */
class TMeshBackplaneServerTest extends TestCase
{
	/** @var string[] Temporary certificate files to remove. */
	private array $tempFiles = [];

	/** @var array<int, TMeshBackplane|TWebSocketServer> What a test opened and tearDown closes, whether or not the test passed. */
	private array $opened = [];

	protected function tearDown(): void
	{
		foreach ($this->opened as $opened) {
			$opened->close();
		}
		$this->opened = [];
		foreach ($this->tempFiles as $file) {
			@unlink($file);
		}
	}

	/**
	 * Writes a self-signed certificate and key for a name to a temporary PEM file.
	 * @param string $name The name the certificate is issued to.
	 * @return string The PEM file path.
	 */
	private function selfSignedCertificate(string $name = '127.0.0.1'): string
	{
		$key = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
		$csr = openssl_csr_new(['commonName' => $name], $key, ['digest_alg' => 'sha256']);
		$cert = openssl_csr_sign($csr, null, $key, 1, ['digest_alg' => 'sha256'], random_int(1, PHP_INT_MAX));
		openssl_x509_export($cert, $certPem);
		openssl_pkey_export($key, $keyPem);
		$file = tempnam(sys_get_temp_dir(), 'wsmesh');
		file_put_contents($file, $certPem . $keyPem);
		$this->tempFiles[] = $file;
		return $file;
	}

	/**
	 * Binds a server whose cluster runs a mesh with the given secret.
	 * @param string $uri The listen URI.
	 * @param mixed $context The listener's stream context.
	 * @return array{0: TWebSocketServer, 1: TMeshBackplane} The server and its mesh.
	 */
	private function meshServer(string $uri, mixed $context = null): array
	{
		$server = TWebSocketServer::bind($uri, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, $context);
		$mesh = new TMeshBackplane();
		$mesh->setSecret('shh');
		$server->setCluster(new TWebSocketCluster('srv', $mesh));
		$server->setHandler(new TWebSocketHandler());
		return [$server, $mesh];
	}

	/**
	 * Pumps the server and the dialer until both hold the link.
	 * @param TWebSocketServer $server
	 * @param TMeshBackplane $serverMesh
	 * @param TMeshBackplane $dialer
	 */
	private function pumpUntilLinked(TWebSocketServer $server, TMeshBackplane $serverMesh, TMeshBackplane $dialer): void
	{
		for ($i = 0; $i < 500 && ($dialer->getPeerCount() === 0 || $serverMesh->getPeerCount() === 0); $i++) {
			$dialer->tick();
			$server->serveOnce(0, 5000);
		}
	}

	public function testATlsMeshDialCompletesAgainstATlsServer()
	{
		if (!extension_loaded('openssl')) {
			self::markTestSkipped('ext-openssl is not available.');
		}
		$certificate = $this->selfSignedCertificate();
		[$server, $serverMesh] = $this->meshServer('tls://127.0.0.1:0', stream_context_create(['ssl' => ['local_cert' => $certificate]]));
		$serverCluster = $server->getCluster();

		$dialer = new TMeshBackplane();
		$dialer->setSecret('shh');
		$dialer->setSslContext(['cafile' => $certificate]);   // the peer name, 127.0.0.1, comes from the URI
		$dialerCluster = new TWebSocketCluster('dialer', $dialer);
		$sink = new TBufferStream();
		$dialerCluster->register(new TWebSocketConnection($sink, false));
		$dialer->connectPeer('tls://127.0.0.1:' . $server->getPort());
		$this->pumpUntilLinked($server, $serverMesh, $dialer);

		self::assertSame(1, $dialer->getPeerCount(), 'The dialer completes the TLS handshake and the upgrade.');
		self::assertSame(1, $serverMesh->getPeerCount(), 'The server holds the peer over TLS.');
		for ($i = 0; $i < 200 && $sink->getSize() === 0; $i++) {
			$serverCluster->broadcast('over tls');   // reaches the dialer once the mutual challenge completes
			$server->serveOnce(0, 5000);
			$dialer->tick();
		}
		self::assertStringContainsString('over tls', (string) $sink, 'Cluster traffic crosses the encrypted link.');

		$dialer->close();
		$server->close();
	}

	public function testATlsMeshDialRefusesAnUntrustedCertificate()
	{
		if (!extension_loaded('openssl')) {
			self::markTestSkipped('ext-openssl is not available.');
		}
		[$server, $serverMesh] = $this->meshServer('tls://127.0.0.1:0', stream_context_create(['ssl' => ['local_cert' => $this->selfSignedCertificate()]]));
		$dialer = new TMeshBackplane();
		$dialer->setSecret('shh');
		$dialer->setSslContext(['cafile' => $this->selfSignedCertificate()]);   // a different authority
		new TWebSocketCluster('dialer', $dialer);
		$dialer->connectPeer('tls://127.0.0.1:' . $server->getPort());
		for ($i = 0; $i < 200 && $dialer->getPendingCount() > 0; $i++) {
			$dialer->tick();
			$server->serveOnce(0, 5000);
		}

		self::assertSame(0, $dialer->getPendingCount(), 'The dial fails at the TLS handshake.');
		self::assertSame(0, $dialer->getPeerCount(), 'No link is made to a peer whose certificate is not trusted.');
		$dialer->close();
		$server->close();
	}

	/**
	 * Dials `tls://localhost` at a TLS mesh server on 127.0.0.1 that presents a certificate for a name.
	 * @param string $name The name on the server's certificate.
	 * @return array{0: TMeshBackplane, 1: TMeshBackplane} The dialer and the server's mesh, pumped until the dial has settled.
	 */
	private function dialLocalhostAgainstACertificateFor(string $name): array
	{
		$certificate = $this->selfSignedCertificate($name);
		[$server, $serverMesh] = $this->meshServer('tls://127.0.0.1:0', stream_context_create(['ssl' => ['local_cert' => $certificate]]));
		$this->opened[] = $server;
		$this->opened[] = $dialer = new TMeshBackplane();
		$dialer->setSecret('shh');
		$dialer->setSslContext(['cafile' => $certificate]);   // the certificate is trusted; only the name is in question
		new TWebSocketCluster('dialer', $dialer);
		$dialer->connectPeer('tls://localhost:' . $server->getPort());   // resolved to 127.0.0.1 by the lookup
		self::assertSame(1, $dialer->getResolvingCount());
		for ($i = 0; $i < 2000 && ($dialer->getResolvingCount() > 0 || $dialer->getPendingCount() > 0 || ($dialer->getPeerCount() > 0 && $serverMesh->getPeerCount() === 0)); $i++) {
			$dialer->tick();
			$server->serveOnce(0, 5000);
		}
		return [$dialer, $serverMesh];
	}

	public function testAHostnameDialVerifiesTheCertificateAgainstTheHostname()
	{
		if (!extension_loaded('openssl')) {
			self::markTestSkipped('ext-openssl is not available.');
		}
		[$dialer, $serverMesh] = $this->dialLocalhostAgainstACertificateFor('localhost');
		self::assertSame(1, $dialer->getPeerCount(), 'The certificate is verified against the hostname in the URI, not the address it resolved to.');
		self::assertSame(1, $serverMesh->getPeerCount());
	}

	public function testAHostnameDialRefusesACertificateForAnotherName()
	{
		if (!extension_loaded('openssl')) {
			self::markTestSkipped('ext-openssl is not available.');
		}
		[$dialer, $serverMesh] = $this->dialLocalhostAgainstACertificateFor('other.example');
		self::assertSame(0, $dialer->getPendingCount(), 'The dial fails at the TLS handshake.');
		self::assertSame(0, $dialer->getPeerCount(), 'A trusted certificate issued to another name is refused.');
		self::assertSame(0, $serverMesh->getPeerCount());
	}

	public function testAnAddressDialRefusesACertificateForAHostname()
	{
		if (!extension_loaded('openssl')) {
			self::markTestSkipped('ext-openssl is not available.');
		}
		$certificate = $this->selfSignedCertificate('localhost');
		[$server, $serverMesh] = $this->meshServer('tls://127.0.0.1:0', stream_context_create(['ssl' => ['local_cert' => $certificate]]));
		$this->opened[] = $server;
		$this->opened[] = $dialer = new TMeshBackplane();
		$dialer->setSecret('shh');
		$dialer->setSslContext(['cafile' => $certificate]);
		new TWebSocketCluster('dialer', $dialer);
		$dialer->connectPeer('tls://127.0.0.1:' . $server->getPort());
		for ($i = 0; $i < 2000 && $dialer->getPendingCount() > 0; $i++) {
			$dialer->tick();
			$server->serveOnce(0, 5000);
		}
		self::assertSame(0, $dialer->getPeerCount(), 'The peer name of a dial by address is the address.');
		self::assertSame(0, $serverMesh->getPeerCount());
	}

	public function testAnAcceptedLinkKeepsTheMeshLimitAboveTheClientLimit()
	{
		[$server, $serverMesh] = $this->meshServer('tcp://127.0.0.1:0');
		$server->setMaxMessageSize(1000);   // the client limit
		$local = new TWebSocketConnection($stream = new TBufferStream(), false);
		$server->getCluster()->register($local);
		$dialer = new TMeshBackplane();
		$dialer->setSecret('shh');
		$dialerCluster = new TWebSocketCluster('dialer', $dialer);
		$dialer->connectPeer('tcp://127.0.0.1:' . $server->getPort());
		$this->pumpUntilLinked($server, $serverMesh, $dialer);
		for ($i = 0; $i < 20; $i++) {   // complete the challenge exchange
			$dialer->tick();
			$server->serveOnce(0, 5000);
		}

		$dialerCluster->broadcast(random_bytes(900), true);   // client-legal, but over 1000 bytes once base64-encoded in an envelope
		for ($i = 0; $i < 50 && $stream->getSize() === 0; $i++) {
			$dialer->tick();
			$server->serveOnce(0, 5000);
		}
		self::assertSame(1, $serverMesh->getPeerCount(), 'The accepted link is not dropped for an envelope over the client limit.');
		self::assertGreaterThan(900, $stream->getSize(), 'The broadcast reaches the server node\'s client.');
		$dialer->close();
		$server->close();
	}

	public function testAPlainMeshDialCompletesThroughTheServer()
	{
		[$server, $serverMesh] = $this->meshServer('tcp://127.0.0.1:0');
		$dialer = new TMeshBackplane();
		$dialer->setSecret('shh');
		new TWebSocketCluster('dialer', $dialer);
		$dialer->connectPeer('tcp://127.0.0.1:' . $server->getPort());
		$this->pumpUntilLinked($server, $serverMesh, $dialer);

		self::assertSame(1, $dialer->getPeerCount());
		self::assertSame(1, $serverMesh->getPeerCount());
		$dialer->close();
		$server->close();
	}
}
