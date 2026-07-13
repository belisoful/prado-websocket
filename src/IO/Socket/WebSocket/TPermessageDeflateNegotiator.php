<?php

/**
 * TPermessageDeflateNegotiator class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\TComponent;

/**
 * TPermessageDeflateNegotiator class.
 *
 * Negotiates the RFC 7692 `permessage-deflate` extension during the opening handshake and produces a
 * configured {@see TPermessageDeflateExtension}.  Add it to {@see TWebSocketServer::setExtensions()}
 * on the server, or to the client handshake options, to offer compression.
 *
 * RFC 7692 negotiates two independent properties per direction: context takeover and the LZ77 window
 * size.  The four parameters map onto the two contexts of each peer:
 *
 * | Parameter | Effect |
 * |---|---|
 * | `server_no_context_takeover` | the server resets its DEFLATE context after each message |
 * | `client_no_context_takeover` | the client resets its DEFLATE context after each message |
 * | `server_max_window_bits` | bounds the server's DEFLATE window |
 * | `client_max_window_bits` | bounds the client's DEFLATE window |
 *
 * As a server, {@see accept()} honors a client's requested limits, narrowing them no further than its
 * own configured policy, and echoes the agreed parameters.  As a client, {@see offer()} advertises
 * the configured policy and {@see fromResponse()} builds the extension from the server's reply.  The
 * receive side always inflates with the maximum window, so a peer's `*_max_window_bits` constrains
 * only the matching send side.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.0.0
 * @see https://www.rfc-editor.org/rfc/rfc7692.html#section-7.1
 */
class TPermessageDeflateNegotiator extends TComponent implements IWebSocketExtensionNegotiator
{
	public const PARAM_SERVER_NO_CONTEXT_TAKEOVER = 'server_no_context_takeover';
	public const PARAM_CLIENT_NO_CONTEXT_TAKEOVER = 'client_no_context_takeover';
	public const PARAM_SERVER_MAX_WINDOW_BITS = 'server_max_window_bits';
	public const PARAM_CLIENT_MAX_WINDOW_BITS = 'client_max_window_bits';

	/** @var bool Whether the server resets its DEFLATE context after each message. */
	private bool $_serverNoContextTakeover;

	/** @var bool Whether the client resets its DEFLATE context after each message. */
	private bool $_clientNoContextTakeover;

	/** @var ?int The server's DEFLATE window limit, in bits, or null for the maximum. */
	private ?int $_serverMaxWindowBits;

	/** @var ?int The client's DEFLATE window limit, in bits, or null for the maximum. */
	private ?int $_clientMaxWindowBits;

	/** @var int The DEFLATE compression level (-1 for the zlib default, 0-9 otherwise). */
	private int $_level;

	/**
	 * @param bool $serverNoContextTakeover Whether the server resets its DEFLATE context per message.
	 * @param bool $clientNoContextTakeover Whether the client resets its DEFLATE context per message.
	 * @param ?int $serverMaxWindowBits The server's DEFLATE window limit (9-15), or null for the maximum.
	 * @param ?int $clientMaxWindowBits The client's DEFLATE window limit (9-15), or null for the maximum.
	 * @param int $level The DEFLATE compression level (-1 for the zlib default).
	 */
	public function __construct(bool $serverNoContextTakeover = false, bool $clientNoContextTakeover = false, ?int $serverMaxWindowBits = null, ?int $clientMaxWindowBits = null, int $level = -1)
	{
		$this->_serverNoContextTakeover = $serverNoContextTakeover;
		$this->_clientNoContextTakeover = $clientNoContextTakeover;
		$this->_serverMaxWindowBits = $serverMaxWindowBits === null ? null : $this->clampWindowBits($serverMaxWindowBits);
		$this->_clientMaxWindowBits = $clientMaxWindowBits === null ? null : $this->clampWindowBits($clientMaxWindowBits);
		$this->_level = $level;
		parent::__construct();
	}

	/**
	 * Returns the extension token, `permessage-deflate`.
	 * @return string The extension name.
	 */
	public function getName(): string
	{
		return TPermessageDeflateExtension::NAME;
	}

	/**
	 * Server side: accepts the first usable client offer, building a configured server extension and
	 * the response parameters to echo.  An offer that demands a window narrower than raw DEFLATE allows
	 * is skipped.
	 * @param array<int, array<string, bool|string>> $offers The offered parameter sets for this token.
	 * @return ?array{0: IWebSocketExtension, 1: array<string, bool|string>} The extension and response
	 *   parameters, or null when no offer is usable.
	 */
	public function accept(array $offers): ?array
	{
		foreach ($offers as $offer) {
			$accepted = $this->acceptOffer($offer);
			if ($accepted !== null) {
				return $accepted;
			}
		}
		return null;
	}

	/**
	 * Builds the server extension and response parameters for one offer, or null when it is unusable.
	 * @param array<string, bool|string> $offer The offered parameters.
	 * @return ?array{0: IWebSocketExtension, 1: array<string, bool|string>} The extension and response.
	 */
	private function acceptOffer(array $offer): ?array
	{
		$response = [];

		$serverNoTakeover = $this->_serverNoContextTakeover || isset($offer[self::PARAM_SERVER_NO_CONTEXT_TAKEOVER]);
		if ($serverNoTakeover) {
			$response[self::PARAM_SERVER_NO_CONTEXT_TAKEOVER] = true;
		}

		$clientNoTakeover = $this->_clientNoContextTakeover || isset($offer[self::PARAM_CLIENT_NO_CONTEXT_TAKEOVER]);
		if ($clientNoTakeover) {
			$response[self::PARAM_CLIENT_NO_CONTEXT_TAKEOVER] = true;
		}

		$serverBits = $this->_serverMaxWindowBits;
		if (isset($offer[self::PARAM_SERVER_MAX_WINDOW_BITS]) && $offer[self::PARAM_SERVER_MAX_WINDOW_BITS] !== true) {
			$requested = (int) $offer[self::PARAM_SERVER_MAX_WINDOW_BITS];
			if ($requested < TPermessageDeflateExtension::MIN_WINDOW_BITS || $requested > TPermessageDeflateExtension::MAX_WINDOW_BITS) {
				return null;   // a window we cannot deflate within; try the next offer
			}
			$serverBits = $serverBits === null ? $requested : min($serverBits, $requested);
		}
		if ($serverBits !== null && $serverBits < TPermessageDeflateExtension::MAX_WINDOW_BITS) {
			$response[self::PARAM_SERVER_MAX_WINDOW_BITS] = (string) $serverBits;
		}

		$extension = new TPermessageDeflateExtension($serverBits ?? TPermessageDeflateExtension::MAX_WINDOW_BITS, $serverNoTakeover, $clientNoTakeover, $this->_level);
		return [$extension, $response];
	}

	/**
	 * Client side: returns the single parameter set to offer for this extension.
	 * @return array<int, array<string, bool|string>> The offered parameter sets.
	 */
	public function offer(): array
	{
		$params = [];
		if ($this->_clientNoContextTakeover) {
			$params[self::PARAM_CLIENT_NO_CONTEXT_TAKEOVER] = true;
		}
		if ($this->_serverNoContextTakeover) {
			$params[self::PARAM_SERVER_NO_CONTEXT_TAKEOVER] = true;
		}
		if ($this->_clientMaxWindowBits !== null) {
			$params[self::PARAM_CLIENT_MAX_WINDOW_BITS] = (string) $this->_clientMaxWindowBits;
		}
		if ($this->_serverMaxWindowBits !== null) {
			$params[self::PARAM_SERVER_MAX_WINDOW_BITS] = (string) $this->_serverMaxWindowBits;
		}
		return [$params];
	}

	/**
	 * Client side: builds the configured extension from the server's accepted parameters.
	 * @param array<string, bool|string> $params The parameters the server accepted.
	 * @return ?IWebSocketExtension The extension, or null when a parameter is unacceptable.
	 */
	public function fromResponse(array $params): ?IWebSocketExtension
	{
		$clientNoTakeover = isset($params[self::PARAM_CLIENT_NO_CONTEXT_TAKEOVER]);
		$serverNoTakeover = isset($params[self::PARAM_SERVER_NO_CONTEXT_TAKEOVER]);

		$deflateWindow = TPermessageDeflateExtension::MAX_WINDOW_BITS;
		if (isset($params[self::PARAM_CLIENT_MAX_WINDOW_BITS]) && $params[self::PARAM_CLIENT_MAX_WINDOW_BITS] !== true) {
			$bits = (int) $params[self::PARAM_CLIENT_MAX_WINDOW_BITS];
			if ($bits < TPermessageDeflateExtension::MIN_WINDOW_BITS || $bits > TPermessageDeflateExtension::MAX_WINDOW_BITS) {
				return null;
			}
			$deflateWindow = $bits;
		}
		if (isset($params[self::PARAM_SERVER_MAX_WINDOW_BITS]) && $params[self::PARAM_SERVER_MAX_WINDOW_BITS] !== true) {
			$bits = (int) $params[self::PARAM_SERVER_MAX_WINDOW_BITS];
			if ($bits < TPermessageDeflateExtension::MIN_WINDOW_BITS || $bits > TPermessageDeflateExtension::MAX_WINDOW_BITS) {
				return null;
			}
		}
		return new TPermessageDeflateExtension($deflateWindow, $clientNoTakeover, $serverNoTakeover, $this->_level);
	}

	/**
	 * Returns whether the server resets its DEFLATE context after each message.
	 * @return bool The server context-takeover policy.
	 */
	public function getServerNoContextTakeover(): bool
	{
		return $this->_serverNoContextTakeover;
	}

	/**
	 * Returns whether the client resets its DEFLATE context after each message.
	 * @return bool The client context-takeover policy.
	 */
	public function getClientNoContextTakeover(): bool
	{
		return $this->_clientNoContextTakeover;
	}

	/**
	 * Returns the server's DEFLATE window limit, in bits, or null for the maximum.
	 * @return ?int The server window limit.
	 */
	public function getServerMaxWindowBits(): ?int
	{
		return $this->_serverMaxWindowBits;
	}

	/**
	 * Returns the client's DEFLATE window limit, in bits, or null for the maximum.
	 * @return ?int The client window limit.
	 */
	public function getClientMaxWindowBits(): ?int
	{
		return $this->_clientMaxWindowBits;
	}

	/**
	 * Returns the DEFLATE compression level.
	 * @return int The level (-1 for the zlib default, 0-9 otherwise).
	 */
	public function getCompressionLevel(): int
	{
		return $this->_level;
	}

	/**
	 * Clamps a window-bit count to the range raw DEFLATE supports.
	 * @param int $bits The requested window bits.
	 * @return int The window bits within 9-15.
	 */
	private function clampWindowBits(int $bits): int
	{
		return max(TPermessageDeflateExtension::MIN_WINDOW_BITS, min(TPermessageDeflateExtension::MAX_WINDOW_BITS, $bits));
	}
}
