<?php

/**
 * TWebSocketService class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\Web\Services;

use Prado\Exceptions\THttpException;
use Prado\IO\Socket\WebSocket\IWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketHandlerTrait;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\TService;
use Prado\Web\THttpHeaderName;

/**
 * TWebSocketService class.
 *
 * The PRADO service that bridges a WebSocket upgrade into the request pipeline.  An upgrade
 * request routed by {@see \Prado\Web\Behaviors\TRequestConnectionUpgrade} selects this service by
 * id; {@see run()} (the PRADO service entry) drives the {@see getConnection() injected connection}
 * through the {@see IWebSocketHandler} message loop.
 *
 * A web SAPI (PHP-FPM, mod_php, FastCGI) cannot serve a WebSocket: the web server owns the client
 * socket and does not hand it to PHP, so the service can only answer the request.  Without an
 * injected connection {@see run()} validates the request as an upgrade and refuses it with the
 * matching status: `400` when it is not a WebSocket upgrade, `426` (with `Sec-WebSocket-Version: 13`)
 * when it asks for another version, and `501` for a valid upgrade the SAPI cannot complete.  Serve
 * WebSockets with the standalone {@see \Prado\IO\Socket\WebSocket\TWebSocketServer} and
 * {@see \Prado\IO\Socket\WebSocket\TWebSocketHandler} instead; a bridge that does own the socket
 * (a custom SAPI or an embedded server) injects the handshaken connection with
 * {@see setConnection()} before the service runs.
 *
 * The service implements the same {@see IWebSocketHandler} role (via {@see TWebSocketHandlerTrait}),
 * so handlers attach to its {@see TWebSocketHandlerTrait::onOpen() onOpen}/
 * {@see TWebSocketHandlerTrait::onMessage() onMessage}/{@see TWebSocketHandlerTrait::onClose()
 * onClose}/{@see TWebSocketHandlerTrait::onError() onError} events the same way.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc6455.html
 */
class TWebSocketService extends TService implements IWebSocketHandler
{
	use TWebSocketHandlerTrait;

	/** @var ?TWebSocketConnection The connection to handle when {@see run()} is invoked. */
	private ?TWebSocketConnection $_connection = null;

	/**
	 * Returns the connection that {@see run()} handles.
	 * @return ?TWebSocketConnection The injected connection, or null.
	 */
	public function getConnection(): ?TWebSocketConnection
	{
		return $this->_connection;
	}

	/**
	 * Sets the connection that {@see run()} handles (supplied by a bridge that owns the socket).
	 * @param ?TWebSocketConnection $value The connection, or null.
	 */
	public function setConnection(?TWebSocketConnection $value): void
	{
		$this->_connection = $value;
	}

	/**
	 * Runs the service: drives the injected connection through {@see handleConnection()}, or, when
	 * none was injected, refuses the SAPI request with the status its upgrade validation earns.
	 * @throws THttpException When no connection was injected: 400, 426, or 501 (see the class description).
	 */
	public function run()
	{
		if ($this->_connection !== null) {
			$this->handleConnection($this->_connection);
			return;
		}
		$this->refuseSapiRequest();
	}

	/**
	 * Refuses the current SAPI request: a request that is not a valid WebSocket upgrade is answered
	 * with its rejection status (400, or 426 naming the supported version), and a valid upgrade with
	 * 501, since the SAPI cannot hand its socket over.
	 * @throws THttpException Always, with the status to send.
	 */
	protected function refuseSapiRequest(): void
	{
		$request = $this->getUpgradeRequest();
		$error = $request === null ? null : TWebSocketHandshake::upgradeError($request);
		if ($error !== null) {
			if (self::rejectionStatus($error) === 426) {
				$this->getResponse()->appendHeader(THttpHeaderName::SecWebSocketVersion . ': ' . TWebSocketHandshake::VERSION);
				throw new THttpException(426, 'websocket_service_version_unsupported', $request['headers'][strtolower(THttpHeaderName::SecWebSocketVersion)] ?? '');
			}
			throw new THttpException(400, 'websocket_service_not_upgrade');
		}
		throw new THttpException(501, 'websocket_service_sapi_unsupported');
	}

	/**
	 * Returns the current application request in the shape {@see TWebSocketHandshake::upgradeError()}
	 * validates: its method, protocol, and lower-cased headers.
	 * @return ?array{method: ?string, protocol: string, headers: array<string, string>} The request,
	 *   or null when no application is running.
	 */
	protected function getUpgradeRequest(): ?array
	{
		$application = $this->getApplication();
		if ($application === null) {
			return null;
		}
		$request = $application->getRequest();
		$headers = [];
		foreach ($request->getHeaders(CASE_LOWER) as $name => $value) {
			$headers[strtolower((string) $name)] = is_array($value) ? implode(', ', $value) : (string) $value;
		}
		return [
			'method' => $request->getRequestType(),
			'protocol' => (string) $request->getHttpProtocolVersion(),
			'headers' => $headers,
		];
	}

	/**
	 * Reads the status code of an HTTP rejection built by {@see TWebSocketHandshake::upgradeError()}.
	 * @param string $rejection The rejection response head.
	 * @return int The status code, or 400 when the head has none.
	 */
	protected static function rejectionStatus(string $rejection): int
	{
		return preg_match('#^HTTP/\d\.\d (\d{3})#', $rejection, $match) === 1 ? (int) $match[1] : 400;
	}
}
