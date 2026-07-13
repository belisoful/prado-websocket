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
	 * Parses a `ws://` or `wss://` URL into the components a client handshake needs: the request
	 * target (`path`, including any query) and the `Host` header value, plus the `secure` flag the
	 * caller uses to choose a TLS transport.  A missing port defaults to 80 (`ws`) or 443 (`wss`),
	 * and the `Host` header omits a default port.
	 * @param string $url The WebSocket URL.
	 * @throws TWebSocketException When the URL is malformed or its scheme is not `ws`/`wss`.
	 * @return array{scheme: string, secure: bool, host: string, port: int, path: string, hostHeader: string}
	 *   The parsed components.
	 */
	public static function parseUrl(string $url): array
	{
		$parts = parse_url($url);
		if (!is_array($parts) || !isset($parts['host'])) {
			throw new TWebSocketException('websocket_url_invalid', $url);
		}
		$scheme = strtolower($parts['scheme'] ?? '');
		if ($scheme !== 'ws' && $scheme !== 'wss') {
			throw new TWebSocketException('websocket_url_scheme_invalid', $scheme);
		}
		$secure = $scheme === 'wss';
		$defaultPort = $secure ? 443 : 80;
		$host = $parts['host'];
		$port = $parts['port'] ?? $defaultPort;
		$path = ($parts['path'] ?? '') === '' ? '/' : $parts['path'];
		if (isset($parts['query'])) {
			$path .= '?' . $parts['query'];
		}
		return [
			'scheme' => $scheme,
			'secure' => $secure,
			'host' => $host,
			'port' => $port,
			'path' => $path,
			'hostHeader' => $port === $defaultPort ? $host : $host . ':' . $port,
		];
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
	 * Indicates whether parsed request headers form a valid WebSocket upgrade: an `Upgrade: websocket`
	 * with `Connection: Upgrade` and a `Sec-WebSocket-Key` that decodes to 16 bytes.
	 * @param array<string, string> $headers The lower-cased request headers.
	 * @return bool Whether the request is a WebSocket upgrade with a valid key.
	 */
	public static function isUpgradeRequest(array $headers): bool
	{
		$connection = array_map('trim', explode(',', strtolower($headers[strtolower(THttpHeaderName::Connection)] ?? '')));
		$key = $headers[strtolower(THttpHeaderName::SecWebSocketKey)] ?? null;
		$decoded = $key === null ? false : base64_decode($key, true);
		return in_array('upgrade', $connection, true)
			&& strtolower($headers[strtolower(THttpHeaderName::Upgrade)] ?? '') === 'websocket'
			&& is_string($decoded) && strlen($decoded) === 16;
	}

	/**
	 * Validates a parsed request as a WebSocket upgrade, returning the HTTP rejection to send when it
	 * is not.  A request that is not an HTTP/1.1-or-higher `GET`, omits `Host`, or is not an
	 * `Upgrade: websocket` with a valid key is a `400`; a request whose `Sec-WebSocket-Version` is not
	 * 13 is a `426` advertising the supported version.
	 * @param array{method: ?string, protocol?: string, headers: array<string, string>} $request The parsed request.
	 * @return ?string The rejection response to write, or null when the request is a valid upgrade.
	 */
	public static function upgradeError(array $request): ?string
	{
		$headers = $request['headers'];
		if (strtoupper((string) ($request['method'] ?? '')) !== 'GET') {
			return self::buildRejection(400, 'Bad Request');
		}
		if (!self::isHttpVersionAtLeast11($request['protocol'] ?? '')) {
			return self::buildRejection(400, 'Bad Request');
		}
		if (!isset($headers[strtolower(THttpHeaderName::Host)])) {
			return self::buildRejection(400, 'Bad Request');
		}
		if (!self::isUpgradeRequest($headers)) {
			return self::buildRejection(400, 'Bad Request');
		}
		if ((int) ($headers[strtolower(THttpHeaderName::SecWebSocketVersion)] ?? 0) !== self::VERSION) {
			return self::buildVersionRejection();
		}
		return null;
	}

	/**
	 * Indicates whether an HTTP request line's protocol is HTTP/1.1 or higher, as RFC 6455 requires
	 * of the upgrade request.
	 * @param string $protocol The request protocol token (e.g. 'HTTP/1.1').
	 * @return bool Whether the protocol is HTTP/1.1 or higher.
	 */
	public static function isHttpVersionAtLeast11(string $protocol): bool
	{
		if (!preg_match('#^HTTP/(\d+)\.(\d+)$#', $protocol, $m)) {
			return false;
		}
		return ((int) $m[1] << 16 | (int) $m[2]) >= (1 << 16 | 1);
	}

	/**
	 * Indicates whether a request's `Origin` is allowed by the configured allowlist.  A null or empty
	 * allowlist permits any origin; otherwise the request must carry an `Origin` the list contains.
	 * @param array<string, string> $headers The lower-cased request headers.
	 * @param ?string[] $allowed The allowed origins, or null to allow any.
	 * @return bool Whether the origin is allowed.
	 */
	public static function isOriginAllowed(array $headers, ?array $allowed): bool
	{
		if ($allowed === null || $allowed === []) {
			return true;
		}
		$origin = $headers[strtolower(THttpHeaderName::Origin)] ?? null;
		return $origin !== null && in_array($origin, $allowed, true);
	}

	/**
	 * Indicates whether a request's `Host` authority is allowed by the configured allowlist.  A null
	 * or empty allowlist permits any host; otherwise the request must carry a `Host` the list contains.
	 * @param array<string, string> $headers The lower-cased request headers.
	 * @param ?string[] $allowed The allowed hosts, or null to allow any.
	 * @return bool Whether the host is allowed.
	 */
	public static function isHostAllowed(array $headers, ?array $allowed): bool
	{
		if ($allowed === null || $allowed === []) {
			return true;
		}
		$host = $headers[strtolower(THttpHeaderName::Host)] ?? null;
		return $host !== null && in_array($host, $allowed, true);
	}

	/**
	 * Selects the subprotocol to use from the client's offer and the server's supported list.
	 * @param array<string, string> $requestHeaders The lower-cased request headers.
	 * @param string[] $supported The subprotocols the server supports, in preference order.
	 * @return ?string The selected subprotocol, or null when none match.
	 */
	public static function negotiateSubprotocol(array $requestHeaders, array $supported): ?string
	{
		if ($supported === []) {
			return null;
		}
		$offered = array_filter(array_map('trim', explode(',', $requestHeaders[strtolower(THttpHeaderName::SecWebSocketProtocol)] ?? '')));
		foreach ($supported as $candidate) {
			if (in_array($candidate, $offered, true)) {
				return $candidate;
			}
		}
		return null;
	}

	/**
	 * Parses a `Sec-WebSocket-Extensions` header into its offers, each a name and its parameters.
	 * A parameter with no value is `true`; a quoted value is unwrapped.  Names and parameter keys are
	 * lower-cased, and a repeated extension keeps every offer in order.
	 * @param string $value The header value.
	 * @return array<int, array{name: string, params: array<string, bool|string>}> The parsed offers.
	 */
	public static function parseExtensionHeader(string $value): array
	{
		$offers = [];
		foreach (self::splitOutsideQuotes($value, ',') as $segment) {
			$parts = self::splitOutsideQuotes(trim($segment), ';');
			$name = strtolower(trim((string) array_shift($parts)));
			if ($name === '') {
				continue;
			}
			$params = [];
			foreach ($parts as $part) {
				$part = trim($part);
				if ($part === '') {
					continue;
				}
				if (str_contains($part, '=')) {
					[$key, $val] = explode('=', $part, 2);
					$val = trim($val);
					if (strlen($val) >= 2 && $val[0] === '"' && $val[-1] === '"') {
						$val = (string) preg_replace('/\\\\(.)/', '$1', substr($val, 1, -1));   // unwrap the quoted-string and undo backslash escapes
					}
					$params[strtolower(trim($key))] = $val;
				} else {
					$params[strtolower($part)] = true;
				}
			}
			$offers[] = ['name' => $name, 'params' => $params];
		}
		return $offers;
	}

	/**
	 * Splits a header value on a delimiter, ignoring delimiters inside a double-quoted string (with
	 * backslash escapes), so a quoted parameter value containing a comma or semicolon is not broken.
	 * @param string $value The header value.
	 * @param string $delimiter The single-character delimiter (',' or ';').
	 * @return string[] The segments.
	 */
	private static function splitOutsideQuotes(string $value, string $delimiter): array
	{
		$parts = [];
		$current = '';
		$inQuotes = false;
		$length = strlen($value);
		for ($i = 0; $i < $length; $i++) {
			$char = $value[$i];
			if ($inQuotes && $char === '\\' && $i + 1 < $length) {
				$current .= $char . $value[$i + 1];   // keep the escaped pair together
				$i++;
			} elseif ($char === '"') {
				$inQuotes = !$inQuotes;
				$current .= $char;
			} elseif ($char === $delimiter && !$inQuotes) {
				$parts[] = $current;
				$current = '';
			} else {
				$current .= $char;
			}
		}
		$parts[] = $current;
		return $parts;
	}

	/**
	 * Formats an extension name and its parameters as a `Sec-WebSocket-Extensions` element.
	 * @param string $name The extension token.
	 * @param array<string, bool|string> $params The parameters; a `true` value is a valueless flag.
	 * @return string The formatted element (e.g. 'permessage-deflate; client_max_window_bits=10').
	 */
	public static function formatExtension(string $name, array $params): string
	{
		$out = $name;
		foreach ($params as $key => $val) {
			$out .= $val === true ? '; ' . $key : '; ' . $key . '=' . $val;
		}
		return $out;
	}

	/**
	 * Server side: negotiates the extensions from the client's offer against the supported negotiators,
	 * in the server's preference order.  An accepted extension whose reserved bit a prior one already
	 * took is skipped, so the agreed list never reserves a bit twice.
	 * @param array<string, string> $requestHeaders The lower-cased request headers.
	 * @param IWebSocketExtensionNegotiator[] $negotiators The supported negotiators, in preference order.
	 * @return array{extensions: IWebSocketExtension[], header: string} The agreed extensions and the
	 *   `Sec-WebSocket-Extensions` response value.
	 */
	public static function negotiateExtensions(array $requestHeaders, array $negotiators): array
	{
		if ($negotiators === []) {
			return ['extensions' => [], 'header' => ''];
		}
		$offers = self::parseExtensionHeader($requestHeaders[strtolower(THttpHeaderName::SecWebSocketExtensions)] ?? '');
		$extensions = [];
		$responses = [];
		$reserved = 0;
		foreach ($negotiators as $negotiator) {
			$name = strtolower($negotiator->getName());
			$matching = [];
			foreach ($offers as $offer) {
				if ($offer['name'] === $name) {
					$matching[] = $offer['params'];
				}
			}
			if ($matching === []) {
				continue;
			}
			$accepted = $negotiator->accept($matching);
			if ($accepted === null) {
				continue;
			}
			[$extension, $params] = $accepted;
			if (($reserved & $extension->getReservedRsv()) !== 0) {
				continue;
			}
			$reserved |= $extension->getReservedRsv();
			$extensions[] = $extension;
			$responses[] = self::formatExtension($negotiator->getName(), $params);
		}
		return ['extensions' => $extensions, 'header' => implode(', ', $responses)];
	}

	/**
	 * Client side: builds the `Sec-WebSocket-Extensions` offer header from the negotiators.
	 * @param IWebSocketExtensionNegotiator[] $negotiators The negotiators to offer.
	 * @return string The offer header value, empty when nothing is offered.
	 */
	public static function offerExtensions(array $negotiators): string
	{
		$offers = [];
		foreach ($negotiators as $negotiator) {
			foreach ($negotiator->offer() as $params) {
				$offers[] = self::formatExtension($negotiator->getName(), $params);
			}
		}
		return implode(', ', $offers);
	}

	/**
	 * Client side: resolves the server's accepted extensions into configured extensions, in the
	 * server's response order.  An accepted extension that was not offered, or whose parameters the
	 * negotiator rejects, fails the handshake.
	 * @param array<string, string> $responseHeaders The lower-cased response headers.
	 * @param IWebSocketExtensionNegotiator[] $negotiators The negotiators that were offered.
	 * @throws TWebSocketException When the server accepted an unoffered or unacceptable extension.
	 * @return IWebSocketExtension[] The configured extensions.
	 */
	public static function resolveExtensions(array $responseHeaders, array $negotiators): array
	{
		$accepted = self::parseExtensionHeader($responseHeaders[strtolower(THttpHeaderName::SecWebSocketExtensions)] ?? '');
		if ($accepted === []) {
			return [];
		}
		$byName = [];
		foreach ($negotiators as $negotiator) {
			$byName[strtolower($negotiator->getName())] = $negotiator;
		}
		$extensions = [];
		foreach ($accepted as $offer) {
			$negotiator = $byName[$offer['name']] ?? null;
			if ($negotiator === null) {
				throw new TWebSocketException('websocket_extension_not_offered', $offer['name']);
			}
			$extension = $negotiator->fromResponse($offer['params']);
			if ($extension === null) {
				throw new TWebSocketException('websocket_extension_unacceptable', $offer['name']);
			}
			$extensions[] = $extension;
		}
		return $extensions;
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
		$headers = $response['headers'] ?? [];
		$connection = array_map('trim', explode(',', strtolower($headers[strtolower(THttpHeaderName::Connection)] ?? '')));
		return ($response['statusCode'] ?? 0) === 101
			&& strtolower($headers[strtolower(THttpHeaderName::Upgrade)] ?? '') === 'websocket'
			&& in_array('upgrade', $connection, true)
			&& ($headers[strtolower(THttpHeaderName::SecWebSocketAccept)] ?? '') === self::acceptKey($sentKey);
	}

	/**
	 * Reads the HTTP handshake head (through the blank line) from a stream.  A total read deadline
	 * bounds how long a slow or dribbling peer can hold the read, so it cannot stall a serve loop.
	 * @param StreamInterface $stream The transport stream.
	 * @param ?float $timeout The total seconds to read the head, or null for no deadline.
	 * @throws TWebSocketException When the head exceeds the limit, the deadline passes, or the stream ends first.
	 * @return string The handshake head, including the terminating blank line.
	 */
	public static function readHandshake(StreamInterface $stream, ?float $timeout = null): string
	{
		$deadline = ($timeout !== null && $timeout > 0) ? microtime(true) + $timeout : null;
		$data = '';
		while (!str_contains($data, "\r\n\r\n")) {
			if (strlen($data) >= self::MAX_HANDSHAKE_BYTES) {
				throw new TWebSocketException('websocket_handshake_too_large', self::MAX_HANDSHAKE_BYTES);
			}
			if ($deadline !== null && microtime(true) >= $deadline) {
				throw new TWebSocketException('websocket_handshake_incomplete');   // a slow/dribbling peer past the deadline
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
	 * Performs the server side: reads the upgrade request, validates it strictly, negotiates the
	 * subprotocol, and writes the 101.  An invalid request is answered with the matching HTTP
	 * rejection (`400`, or `426` for a version mismatch) before the exception.
	 * When `origins` is set, the request's `Origin` must be in the list or the upgrade is refused with
	 * a `403`.  An unset or empty `origins` allows any origin.
	 * @param StreamInterface $stream The accepted transport stream.
	 * @param array{subprotocols?: string[], extensions?: IWebSocketExtensionNegotiator[], origins?: string[], allowedHosts?: string[], headers?: array<string, string>} $options
	 *   The supported subprotocols, extension negotiators, allowed origins, and extra response headers.
	 * @throws TWebSocketException When the request is not a valid WebSocket upgrade or the origin is rejected.
	 * @return array{method: ?string, target: ?string, headers: array<string, string>, subprotocol: ?string, extensions: IWebSocketExtension[]}
	 *   The parsed request with the negotiated subprotocol and extensions.
	 */
	public static function acceptConnection(StreamInterface $stream, array $options = []): array
	{
		$request = self::parseHttpMessage(self::readHandshake($stream));
		$error = self::upgradeError($request);
		if ($error !== null) {
			$stream->write($error);
			throw new TWebSocketException('websocket_handshake_not_upgrade');
		}
		$headers = $request['headers'];
		if (!self::isOriginAllowed($headers, $options['origins'] ?? null)) {
			$stream->write(self::buildRejection(403, 'Forbidden'));
			throw new TWebSocketException('websocket_handshake_origin_rejected', $headers[strtolower(THttpHeaderName::Origin)] ?? '');
		}
		if (!self::isHostAllowed($headers, $options['allowedHosts'] ?? null)) {
			$stream->write(self::buildRejection(400, 'Bad Request'));
			throw new TWebSocketException('websocket_handshake_not_upgrade');   // the Host is not one this server serves
		}
		$subprotocol = self::negotiateSubprotocol($headers, $options['subprotocols'] ?? []);
		$negotiated = self::negotiateExtensions($headers, $options['extensions'] ?? []);

		$responseHeaders = $options['headers'] ?? [];
		if ($subprotocol !== null) {
			$responseHeaders[THttpHeaderName::SecWebSocketProtocol] = $subprotocol;
		}
		if ($negotiated['header'] !== '') {
			$responseHeaders[THttpHeaderName::SecWebSocketExtensions] = $negotiated['header'];
		}
		$stream->write(self::buildServerResponse($headers[strtolower(THttpHeaderName::SecWebSocketKey)], $responseHeaders));
		return $request + ['subprotocol' => $subprotocol, 'extensions' => $negotiated['extensions']];
	}

	/**
	 * Reads and validates an upgrade request without responding, so a caller can authorize it before
	 * completing the handshake.  Pair with {@see buildServerResponse()} to accept or
	 * {@see buildRejection()} / {@see upgradeError()} to refuse.
	 * @param StreamInterface $stream The accepted transport stream.
	 * @param ?float $timeout The total seconds to read the request head, or null for no deadline.
	 * @throws TWebSocketException When the request is not a valid WebSocket upgrade.
	 * @return array{requestLine: string, method: ?string, target: ?string, protocol: string, statusCode: ?int, headers: array<string, string>, body: string}
	 *   The parsed request.
	 */
	public static function receiveRequest(StreamInterface $stream, ?float $timeout = null): array
	{
		$request = self::parseHttpMessage(self::readHandshake($stream, $timeout));
		if (self::upgradeError($request) !== null) {
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
		return "HTTP/1.1 {$status} {$reason}\r\n" . THttpHeaderName::Connection . ": close\r\nContent-Length: 0\r\n\r\n";
	}

	/**
	 * Builds the `426 Upgrade Required` response that advertises the supported WebSocket version.
	 * @return string The response head.
	 */
	public static function buildVersionRejection(): string
	{
		return "HTTP/1.1 426 Upgrade Required\r\n"
			. THttpHeaderName::SecWebSocketVersion . ': ' . self::VERSION . "\r\n"
			. THttpHeaderName::Connection . ": close\r\nContent-Length: 0\r\n\r\n";
	}

	/**
	 * Performs the client side: sends the upgrade request (offering subprotocols) and verifies the
	 * server's 101, reading back what was selected.
	 * @param StreamInterface $stream The connected transport stream.
	 * @param string $host The Host header value.
	 * @param string $path The request target. Default '/'.
	 * @param array{subprotocols?: string[], extensions?: IWebSocketExtensionNegotiator[], headers?: array<string, string>} $options
	 *   The subprotocols to offer, extension negotiators, and extra request headers.
	 * @throws TWebSocketException When the server does not complete the handshake.
	 * @return array{statusCode: ?int, headers: array<string, string>, subprotocol: ?string, extensions: IWebSocketExtension[]}
	 *   The parsed response with the selected subprotocol and extensions.
	 */
	public static function openConnection(StreamInterface $stream, string $host, string $path = '/', array $options = []): array
	{
		$key = self::generateKey();
		$requestHeaders = $options['headers'] ?? [];
		$subprotocols = $options['subprotocols'] ?? [];
		if ($subprotocols !== []) {
			$requestHeaders[THttpHeaderName::SecWebSocketProtocol] = implode(', ', $subprotocols);
		}
		$negotiators = $options['extensions'] ?? [];
		$offer = self::offerExtensions($negotiators);
		if ($offer !== '') {
			$requestHeaders[THttpHeaderName::SecWebSocketExtensions] = $offer;
		}
		$stream->write(self::buildClientRequest($host, $path, $key, $requestHeaders));
		$response = self::parseHttpMessage(self::readHandshake($stream));
		if (!self::verifyServerResponse($response, $key)) {
			throw new TWebSocketException('websocket_handshake_rejected', $response['statusCode'] ?? 0);
		}
		$subprotocol = $response['headers'][strtolower(THttpHeaderName::SecWebSocketProtocol)] ?? null;
		if ($subprotocol !== null && $subprotocol !== '' && !in_array($subprotocol, $subprotocols, true)) {
			throw new TWebSocketException('websocket_subprotocol_not_offered', $subprotocol);   // RFC 6455 s4.1: fail on a subprotocol the client did not offer
		}
		$extensions = self::resolveExtensions($response['headers'], $negotiators);
		return $response + ['subprotocol' => $subprotocol, 'extensions' => $extensions];
	}
}
