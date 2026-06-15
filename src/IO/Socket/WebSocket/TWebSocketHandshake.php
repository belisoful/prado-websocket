<?php

/**
 * TWebSocketHandshake class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\Web\THttpHeaderName;
use Psr\Http\Message\StreamInterface;

/**
 * TWebSocketHandshake class.
 *
 * Performs the RFC 6455 opening handshake.  The pure functions compute the accept key
 * ({@see acceptKey()}), generate a client key ({@see generateKey()}), parse an HTTP head
 * ({@see parseHttpMessage()}), and build the request/response ({@see buildClientRequest()},
 * {@see buildServerResponse()}).  The stream functions drive a transport end to end:
 * {@see acceptConnection()} reads the upgrade request, validates it, and writes the 101
 * response; {@see openConnection()} sends the request and verifies the server's 101.
 *
 * The accept key is `base64(sha1(Sec-WebSocket-Key . GUID))`, where the GUID is the fixed
 * RFC 6455 value.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 4.4.0
 * @see https://www.rfc-editor.org/rfc/rfc6455.html#section-4
 */
class TWebSocketHandshake
{
	/** @var string The fixed RFC 6455 GUID appended to the key before hashing. */
	public const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

	/** @var int The WebSocket protocol version this implements. */
	public const VERSION = 13;

	/** @var int The maximum handshake head size read from a stream. */
	public const MAX_HANDSHAKE_BYTES = 16384;

	/**
	 * Computes the Sec-WebSocket-Accept value for a client's Sec-WebSocket-Key.
	 * @param string $key The client's Sec-WebSocket-Key.
	 * @return string The base64 accept value.
	 */
	public static function acceptKey(string $key): string
	{
		return base64_encode(sha1(trim($key) . self::GUID, true));
	}

	/**
	 * Generates a random 16-byte client Sec-WebSocket-Key, base64-encoded.
	 * @return string The client key.
	 */
	public static function generateKey(): string
	{
		return base64_encode(random_bytes(16));
	}

	/**
	 * Parses an HTTP request or response head into its line, headers, and body.
	 * @param string $data The HTTP head (and optional body) text.
	 * @return array{requestLine: string, method: ?string, target: ?string, protocol: string, statusCode: ?int, headers: array<string, string>, body: string}
	 *   The parsed message; header keys are lower-cased.
	 */
	public static function parseHttpMessage(string $data): array
	{
		$split = strpos($data, "\r\n\r\n");
		$head = $split === false ? $data : substr($data, 0, $split);
		$body = $split === false ? '' : substr($data, $split + 4);

		$lines = explode("\r\n", $head);
		$first = array_shift($lines) ?? '';
		$parts = explode(' ', $first, 3);

		$result = [
			'requestLine' => $first,
			'method' => null,
			'target' => null,
			'protocol' => '',
			'statusCode' => null,
			'headers' => [],
			'body' => $body,
		];
		if (str_starts_with($first, 'HTTP/')) {
			$result['protocol'] = $parts[0] ?? '';
			$result['statusCode'] = isset($parts[1]) ? (int) $parts[1] : null;
		} else {
			$result['method'] = $parts[0] ?? null;
			$result['target'] = $parts[1] ?? null;
			$result['protocol'] = $parts[2] ?? '';
		}
		foreach ($lines as $line) {
			if ($line === '' || !str_contains($line, ':')) {
				continue;
			}
			[$name, $value] = explode(':', $line, 2);
			$result['headers'][strtolower(trim($name))] = trim($value);
		}
		return $result;
	}

	/**
	 * Indicates whether parsed request headers form a valid WebSocket upgrade.
	 * @param array<string, string> $headers The lower-cased request headers.
	 * @return bool Whether the request is a WebSocket upgrade with a key.
	 */
	public static function isUpgradeRequest(array $headers): bool
	{
		$connection = array_map('trim', explode(',', strtolower($headers[strtolower(THttpHeaderName::Connection)] ?? '')));
		return in_array('upgrade', $connection, true)
			&& strtolower($headers[strtolower(THttpHeaderName::Upgrade)] ?? '') === 'websocket'
			&& isset($headers[strtolower(THttpHeaderName::SecWebSocketKey)]);
	}

	/**
	 * Builds the 101 Switching Protocols response for a client key.
	 * @param string $key The client's Sec-WebSocket-Key.
	 * @param array<string, string> $extraHeaders Extra response headers (e.g. Sec-WebSocket-Protocol).
	 * @return string The response head.
	 */
	public static function buildServerResponse(string $key, array $extraHeaders = []): string
	{
		$headers = [
			THttpHeaderName::Upgrade => 'websocket',
			THttpHeaderName::Connection => 'Upgrade',
			THttpHeaderName::SecWebSocketAccept => self::acceptKey($key),
		] + $extraHeaders;
		$response = "HTTP/1.1 101 Switching Protocols\r\n";
		foreach ($headers as $name => $value) {
			$response .= $name . ': ' . $value . "\r\n";
		}
		return $response . "\r\n";
	}

	/**
	 * Builds the client GET upgrade request.
	 * @param string $host The Host header value.
	 * @param string $path The request target. Default '/'.
	 * @param string $key The Sec-WebSocket-Key.
	 * @param array<string, string> $extraHeaders Extra request headers.
	 * @return string The request head.
	 */
	public static function buildClientRequest(string $host, string $path, string $key, array $extraHeaders = []): string
	{
		$headers = [
			THttpHeaderName::Host => $host,
			THttpHeaderName::Upgrade => 'websocket',
			THttpHeaderName::Connection => 'Upgrade',
			THttpHeaderName::SecWebSocketKey => $key,
			THttpHeaderName::SecWebSocketVersion => (string) self::VERSION,
		] + $extraHeaders;
		$request = 'GET ' . ($path === '' ? '/' : $path) . " HTTP/1.1\r\n";
		foreach ($headers as $name => $value) {
			$request .= $name . ': ' . $value . "\r\n";
		}
		return $request . "\r\n";
	}

	/**
	 * Verifies a parsed server response against the key the client sent.
	 * @param array{statusCode: ?int, headers: array<string, string>} $response The parsed response.
	 * @param string $sentKey The Sec-WebSocket-Key the client sent.
	 * @return bool Whether the response is a valid 101 with the matching accept value.
	 */
	public static function verifyServerResponse(array $response, string $sentKey): bool
	{
		return ($response['statusCode'] ?? 0) === 101
			&& ($response['headers'][strtolower(THttpHeaderName::SecWebSocketAccept)] ?? '') === self::acceptKey($sentKey);
	}

	/**
	 * Reads the HTTP handshake head (through the blank line) from a stream.
	 * @param StreamInterface $stream The transport stream.
	 * @throws TWebSocketException When the head exceeds the limit or the stream ends first.
	 * @return string The handshake head, including the terminating blank line.
	 */
	public static function readHandshake(StreamInterface $stream): string
	{
		$data = '';
		while (!str_contains($data, "\r\n\r\n")) {
			if (strlen($data) >= self::MAX_HANDSHAKE_BYTES) {
				throw new TWebSocketException('websocket_handshake_too_large', self::MAX_HANDSHAKE_BYTES);
			}
			$byte = $stream->eof() ? '' : $stream->read(1);
			if ($byte === '') {
				throw new TWebSocketException('websocket_handshake_incomplete');
			}
			$data .= $byte;
		}
		return $data;
	}

	/**
	 * Performs the server side: reads the upgrade request, validates it, and writes the 101.
	 * @param StreamInterface $stream The accepted transport stream.
	 * @param array<string, string> $extraHeaders Extra response headers.
	 * @throws TWebSocketException When the request is not a valid WebSocket upgrade.
	 * @return array{requestLine: string, method: ?string, target: ?string, protocol: string, statusCode: ?int, headers: array<string, string>, body: string}
	 *   The parsed request.
	 */
	public static function acceptConnection(StreamInterface $stream, array $extraHeaders = []): array
	{
		$request = self::receiveRequest($stream);
		$stream->write(self::buildServerResponse($request['headers'][strtolower(THttpHeaderName::SecWebSocketKey)], $extraHeaders));
		return $request;
	}

	/**
	 * Reads and validates an upgrade request without responding, so a caller can authorize it before
	 * completing the handshake.  Pair with {@see buildServerResponse()} to accept or
	 * {@see buildRejection()} to refuse.
	 * @param StreamInterface $stream The accepted transport stream.
	 * @throws TWebSocketException When the request is not a valid WebSocket upgrade.
	 * @return array{requestLine: string, method: ?string, target: ?string, protocol: string, statusCode: ?int, headers: array<string, string>, body: string}
	 *   The parsed request.
	 */
	public static function receiveRequest(StreamInterface $stream): array
	{
		$request = self::parseHttpMessage(self::readHandshake($stream));
		if (!self::isUpgradeRequest($request['headers'])) {
			throw new TWebSocketException('websocket_handshake_not_upgrade');
		}
		return $request;
	}

	/**
	 * Builds an HTTP error response that refuses an upgrade before the 101.
	 * @param int $status The status code.
	 * @param string $reason The reason phrase.
	 * @return string The response head.
	 */
	public static function buildRejection(int $status = 403, string $reason = 'Forbidden'): string
	{
		return "HTTP/1.1 {$status} {$reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
	}

	/**
	 * Performs the client side: sends the upgrade request and verifies the server's 101.
	 * @param StreamInterface $stream The connected transport stream.
	 * @param string $host The Host header value.
	 * @param string $path The request target. Default '/'.
	 * @param array<string, string> $extraHeaders Extra request headers.
	 * @throws TWebSocketException When the server does not complete the handshake.
	 * @return array{requestLine: string, method: ?string, target: ?string, protocol: string, statusCode: ?int, headers: array<string, string>, body: string}
	 *   The parsed response.
	 */
	public static function openConnection(StreamInterface $stream, string $host, string $path = '/', array $extraHeaders = []): array
	{
		$key = self::generateKey();
		$stream->write(self::buildClientRequest($host, $path, $key, $extraHeaders));
		$response = self::parseHttpMessage(self::readHandshake($stream));
		if (!self::verifyServerResponse($response, $key)) {
			throw new TWebSocketException('websocket_handshake_rejected', $response['statusCode'] ?? 0);
		}
		return $response;
	}
}
