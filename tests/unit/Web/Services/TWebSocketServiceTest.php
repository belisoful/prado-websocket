<?php

namespace Prado\Test\Unit\Web\Services;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\THttpException;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketMessage;
use Prado\Prado;
use Prado\TApplication;
use Prado\Web\Services\TWebSocketService;
use Prado\Web\THttpRequest;
use Prado\Web\THttpResponse;

/** A response that records the headers the service appends, since the CLI cannot send them. */
class HeaderRecordingResponse extends THttpResponse
{
	/** @var string[] */
	public array $headers = [];

	public function appendHeader($header, bool $replace = true, int $response_code = 0): void
	{
		$this->headers[] = $header;
	}
}

/** A service fed a canned SAPI request, so every refusal path runs without a web server. */
class CannedRequestWebSocketService extends TWebSocketService
{
	public ?array $request = null;
	public HeaderRecordingResponse $response;

	public function __construct()
	{
		$this->response = new HeaderRecordingResponse();
		parent::__construct();
	}

	protected function getUpgradeRequest(): ?array
	{
		return $this->request;
	}

	public function getResponse()
	{
		return $this->response;
	}
}

/** A request whose method, protocol, and headers are set by the test instead of read from $_SERVER. */
class CannedHttpRequest extends THttpRequest
{
	public string $method = 'GET';
	public ?string $protocol = 'HTTP/1.1';
	/** @var array<string, string> */
	public array $headers = [];

	public function getRequestType()
	{
		return $this->method;
	}

	public function getHttpProtocolVersion()
	{
		return $this->protocol;
	}

	public function getHeaders($case = null)
	{
		return $case === null ? $this->headers : array_change_key_case($this->headers, $case);
	}
}

class TWebSocketServiceTest extends TestCase
{
	/** @return array{0: TWebSocketConnection, 1: TWebSocketConnection, 2: TSocketStream, 3: TSocketStream} */
	private function pair(): array
	{
		[$a, $b] = TSocketStream::pair();
		return [new TWebSocketConnection($a, true), new TWebSocketConnection($b, false), $a, $b];
	}

	/** @return array<string, string> A valid upgrade request's headers, lower-cased. */
	private function upgradeHeaders(): array
	{
		return [
			'host' => 'example.com',
			'upgrade' => 'websocket',
			'connection' => 'Upgrade',
			'sec-websocket-key' => 'dGhlIHNhbXBsZSBub25jZQ==',
			'sec-websocket-version' => '13',
		];
	}

	public function testHandleConnectionRaisesOpenMessageClose()
	{
		[$client, $server, $a, $b] = $this->pair();
		$client->send('one');
		$client->sendBinary('two');
		$client->close(1000);

		$service = new TWebSocketService();
		$events = [];
		$service->attachEventHandler('onOpen', function ($conn) use (&$events) {
			$events[] = ['open', $conn];
		});
		$service->attachEventHandler('onMessage', function ($conn, $msg) use (&$events) {
			self::assertInstanceOf(TWebSocketMessage::class, $msg, 'The onMessage event carries the message object.');
			$events[] = ['message', (string) $msg, $msg->getIsBinary()];
		});
		$service->attachEventHandler('onClose', function ($conn) use (&$events) {
			$events[] = ['close', $conn];
		});

		$service->handleConnection($server);

		self::assertSame('open', $events[0][0]);
		self::assertSame($server, $events[0][1]);
		self::assertSame(['message', 'one', false], $events[1]);
		self::assertSame(['message', 'two', true], $events[2]);
		self::assertSame('close', $events[3][0]);
		self::assertTrue($server->getIsClosed());
		$a->close();
		$b->close();
	}

	public function testRunHandlesInjectedConnection()
	{
		[$client, $server, $a, $b] = $this->pair();
		$client->send('hi');
		$client->close(1000);

		$service = new TWebSocketService();
		$service->setConnection($server);
		self::assertSame($server, $service->getConnection());

		$messages = [];
		$service->attachEventHandler('onMessage', function ($conn, $msg) use (&$messages) {
			$messages[] = (string) $msg;
		});
		$service->run();

		self::assertSame(['hi'], $messages);
		$a->close();
		$b->close();
	}

	public function testRunWithoutAConnectionOrApplicationAnswers501()
	{
		if (Prado::getApplication() !== null) {
			$this->markTestSkipped('An application is registered in this process.');
		}
		$service = new TWebSocketService();
		self::assertNull($service->getConnection());
		try {
			$service->run();
			self::fail('A SAPI request cannot be served.');
		} catch (THttpException $e) {
			self::assertSame(501, $e->getStatusCode(), 'Without a request to inspect, the SAPI cannot serve the upgrade.');
			self::assertSame('websocket_service_sapi_unsupported', $e->getErrorCode());
		}
	}

	public function testRunRefusesARequestThatIsNotAnUpgradeWith400()
	{
		$service = new CannedRequestWebSocketService();
		$service->request = ['method' => 'GET', 'protocol' => 'HTTP/1.1', 'headers' => ['host' => 'example.com', 'accept' => 'text/html']];
		try {
			$service->run();
			self::fail('A plain request is not a WebSocket upgrade.');
		} catch (THttpException $e) {
			self::assertSame(400, $e->getStatusCode());
			self::assertSame('websocket_service_not_upgrade', $e->getErrorCode());
		}

		$service->request = ['method' => 'POST', 'protocol' => 'HTTP/1.1', 'headers' => $this->upgradeHeaders()];
		try {
			$service->run();
			self::fail('An upgrade must be a GET.');
		} catch (THttpException $e) {
			self::assertSame(400, $e->getStatusCode());
		}
		self::assertSame([], $service->response->headers, 'A 400 carries no version header.');
	}

	public function testRunRefusesAnUnsupportedVersionWith426()
	{
		$service = new CannedRequestWebSocketService();
		$service->request = ['method' => 'GET', 'protocol' => 'HTTP/1.1', 'headers' => ['sec-websocket-version' => '8'] + $this->upgradeHeaders()];
		try {
			$service->run();
			self::fail('Version 8 is not supported.');
		} catch (THttpException $e) {
			self::assertSame(426, $e->getStatusCode());
			self::assertSame('websocket_service_version_unsupported', $e->getErrorCode());
			self::assertStringContainsString("'8'", $e->getMessage(), 'The message names the requested version.');
		}
		self::assertSame(['Sec-WebSocket-Version: 13'], $service->response->headers, 'A 426 advertises the supported version.');
	}

	public function testRunRefusesAValidUpgradeWith501BecauseTheSapiOwnsTheSocket()
	{
		$service = new CannedRequestWebSocketService();
		$service->request = ['method' => 'GET', 'protocol' => 'HTTP/1.1', 'headers' => $this->upgradeHeaders()];
		try {
			$service->run();
			self::fail('A web SAPI cannot hand its socket over.');
		} catch (THttpException $e) {
			self::assertSame(501, $e->getStatusCode());
			self::assertSame('websocket_service_sapi_unsupported', $e->getErrorCode());
			self::assertStringContainsString('TWebSocketServer', $e->getMessage(), 'The message points at the standalone server.');
		}
	}

	public function testRunReadsTheApplicationRequest()
	{
		$application = Prado::getApplication();
		if ($application === null) {
			$base = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'wsapp_' . uniqid('', true);
			mkdir($base);
			mkdir($base . DIRECTORY_SEPARATOR . 'runtime');
			$application = new TApplication($base, false);
		}
		$request = new CannedHttpRequest();
		$application->setRequest($request);
		$service = new TWebSocketService();

		$request->headers = ['Host' => 'example.com', 'Accept' => 'text/html'];   // as a SAPI reports them, mixed case
		try {
			$service->run();
			self::fail('A plain application request is refused.');
		} catch (THttpException $e) {
			self::assertSame(400, $e->getStatusCode(), 'The service validates the application request.');
		}

		$request->headers = array_change_key_case($this->upgradeHeaders(), CASE_UPPER);
		try {
			$service->run();
			self::fail('A valid upgrade through the SAPI is refused.');
		} catch (THttpException $e) {
			self::assertSame(501, $e->getStatusCode(), 'Header names are matched case-insensitively.');
		}
	}

	public function testProtocolErrorRaisesErrorAndCloses()
	{
		[$a, $b] = TSocketStream::pair();
		$server = new TWebSocketConnection($b, false);
		// A control frame with a payload over 125 bytes is a protocol error on decode.
		$a->write("\x89\x7e\x00\x80" . str_repeat('x', 128));

		$service = new TWebSocketService();
		$error = null;
		$service->attachEventHandler('onError', function ($conn, $e) use (&$error) {
			$error = $e;
		});
		$service->handleConnection($server);

		self::assertInstanceOf(\Prado\IO\Socket\WebSocket\TWebSocketException::class, $error);
		self::assertTrue($server->getIsClosing() || $server->getIsClosed());
		$a->close();
		$b->close();
	}
}
