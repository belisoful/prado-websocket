<?php

/**
 * IWebSocketExtensionNegotiator interface file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

/**
 * IWebSocketExtensionNegotiator interface.
 *
 * Negotiates one RFC 6455 extension during the opening handshake.  A negotiator owns an extension
 * token (`permessage-deflate`, for example) and turns the `Sec-WebSocket-Extensions` exchange into a
 * configured {@see IWebSocketExtension} the connection runs.  Negotiation is per-extension because
 * each extension parses its own parameters; {@see TWebSocketHandshake} drives a list of negotiators
 * and hands the agreed extensions to {@see TWebSocketConnection::setExtensions()}.
 *
 * A negotiator is a factory, separate from the {@see IWebSocketExtension} transform it produces, so
 * the same extension can be configured from either side of the handshake.  Parameters are a map of
 * lower-cased name to string value, or `true` for a valueless flag.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc6455.html#section-9.1
 */
interface IWebSocketExtensionNegotiator
{
	/**
	 * Returns the extension token this negotiator handles.
	 * @return string The extension name (e.g. 'permessage-deflate').
	 */
	public function getName(): string;

	/**
	 * Server side: accepts one of the client's offers for this extension, or declines.  The client
	 * may offer the extension more than once with different parameters, given in client order.
	 * @param array<int, array<string, bool|string>> $offers The offered parameter sets for this token.
	 * @return ?array{0: IWebSocketExtension, 1: array<string, bool|string>} The accepted extension and
	 *   the response parameters to echo, or null to decline.
	 */
	public function accept(array $offers): ?array;

	/**
	 * Client side: returns the parameter sets to offer for this extension, one entry per offer.
	 * @return array<int, array<string, bool|string>> The offers, empty to offer nothing.
	 */
	public function offer(): array;

	/**
	 * Client side: builds the configured extension from the server's accepted parameters, or rejects.
	 * @param array<string, bool|string> $params The parameters the server accepted for this token.
	 * @return ?IWebSocketExtension The configured extension, or null when the parameters are unacceptable.
	 */
	public function fromResponse(array $params): ?IWebSocketExtension;
}
