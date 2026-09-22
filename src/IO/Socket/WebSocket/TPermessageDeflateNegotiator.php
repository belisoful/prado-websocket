<?php

/**
 * TPermessageDeflateNegotiator class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
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
 * own configured policy, and echoes the agreed parameters.  An offer is declined (RFC 7692 §7.1) when
 * it carries an unknown parameter, a flag with a value, a `server_max_window_bits` without a value,
 * a window value that is not an integer 8-15, or a server window below what raw DEFLATE supports;
 * the next offer for the token is tried.  {@see TWebSocketHandshake::negotiateExtensions()} declines
 * an offer with a repeated parameter before it reaches the negotiator.
 *
 * As a client, {@see offer()} advertises the configured policy and {@see fromResponse()} builds the
 * extension from the server's reply.  The client is bound by its own offered
 * `client_no_context_takeover` and `client_max_window_bits` even when the response omits them
 * (§7.1.1.2, §7.1.2.2).  A response fails the handshake when it carries an unknown parameter, a
 * flag with a value, `client_max_window_bits` the client never offered or without a value, a
 * `server_max_window_bits` without a value or larger than the client requested, a client window
 * raw DEFLATE cannot produce, or omits a `server_no_context_takeover` the client offered.
 *
 * The receive side always inflates with the maximum window, so a peer's `*_max_window_bits`
 * constrains only the matching send side.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc7692.html#section-7.1
 */
class TPermessageDeflateNegotiator extends TComponent implements IWebSocketExtensionNegotiator
{
	public const PARAM_SERVER_NO_CONTEXT_TAKEOVER = 'server_no_context_takeover';
	public const PARAM_CLIENT_NO_CONTEXT_TAKEOVER = 'client_no_context_takeover';
	public const PARAM_SERVER_MAX_WINDOW_BITS = 'server_max_window_bits';
	public const PARAM_CLIENT_MAX_WINDOW_BITS = 'client_max_window_bits';

	/** The smallest window value the parameters admit (RFC 7692 §7.1.2); raw DEFLATE compresses with 9 at minimum. */
	public const PARAM_MIN_WINDOW_BITS = 8;

	/** The parameters RFC 7692 defines; any other parameter declines an offer or fails a response. */
	private const KNOWN_PARAMS = [
		self::PARAM_SERVER_NO_CONTEXT_TAKEOVER,
		self::PARAM_CLIENT_NO_CONTEXT_TAKEOVER,
		self::PARAM_SERVER_MAX_WINDOW_BITS,
		self::PARAM_CLIENT_MAX_WINDOW_BITS,
	];

	/** The parameters that carry no value. */
	private const FLAG_PARAMS = [self::PARAM_SERVER_NO_CONTEXT_TAKEOVER, self::PARAM_CLIENT_NO_CONTEXT_TAKEOVER];

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
	 * @param int $level The DEFLATE compression level (-1 for the zlib default, 0-9 otherwise).
	 * @throws \Prado\Exceptions\TInvalidDataValueException When the level is not -1 or 0-9.
	 */
	public function __construct(bool $serverNoContextTakeover = false, bool $clientNoContextTakeover = false, ?int $serverMaxWindowBits = null, ?int $clientMaxWindowBits = null, int $level = -1)
	{
		$this->_serverNoContextTakeover = $serverNoContextTakeover;
		$this->_clientNoContextTakeover = $clientNoContextTakeover;
		$this->_serverMaxWindowBits = $serverMaxWindowBits === null ? null : $this->clampWindowBits($serverMaxWindowBits);
		$this->_clientMaxWindowBits = $clientMaxWindowBits === null ? null : $this->clampWindowBits($clientMaxWindowBits);
		$this->_level = TPermessageDeflateExtension::ensureLevel($level);
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
	 * the response parameters to echo.  An offer RFC 7692 §7.1 requires declining, or that demands a
	 * server window narrower than raw DEFLATE allows, is skipped for the next.
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
	 * Builds the server extension and response parameters for one offer, or null to decline it.
	 * @param array<string, bool|string> $offer The offered parameters.
	 * @return ?array{0: IWebSocketExtension, 1: array<string, bool|string>} The extension and response.
	 */
	private function acceptOffer(array $offer): ?array
	{
		if (!self::isWellFormed($offer)) {
			return null;
		}
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
		if (array_key_exists(self::PARAM_SERVER_MAX_WINDOW_BITS, $offer)) {
			$requested = self::parseWindowBits($offer[self::PARAM_SERVER_MAX_WINDOW_BITS]);   // null for a bare or invalid value
			if ($requested === null || $requested < TPermessageDeflateExtension::MIN_WINDOW_BITS) {
				return null;   // malformed, or a window we cannot deflate within; try the next offer
			}
			$serverBits = min($serverBits ?? TPermessageDeflateExtension::MAX_WINDOW_BITS, $requested);
		}
		if ($serverBits !== null && $serverBits < TPermessageDeflateExtension::MAX_WINDOW_BITS) {
			$response[self::PARAM_SERVER_MAX_WINDOW_BITS] = (string) $serverBits;
		}

		if (array_key_exists(self::PARAM_CLIENT_MAX_WINDOW_BITS, $offer)) {
			$value = $offer[self::PARAM_CLIENT_MAX_WINDOW_BITS];
			$offeredBits = $value === true ? TPermessageDeflateExtension::MAX_WINDOW_BITS : self::parseWindowBits($value);   // bare means the client accepts any bound
			if ($offeredBits === null) {
				return null;
			}
			if ($this->_clientMaxWindowBits !== null) {
				$response[self::PARAM_CLIENT_MAX_WINDOW_BITS] = (string) min($this->_clientMaxWindowBits, $offeredBits);   // the policy may only be imposed when the client offered the parameter
			}
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
	 * Client side: builds the configured extension from the server's accepted parameters.  The
	 * client's own offered `client_no_context_takeover` and `client_max_window_bits` apply whether or
	 * not the server echoes them.
	 * @param array<string, bool|string> $params The parameters the server accepted.
	 * @return ?IWebSocketExtension The extension, or null when the response must fail the handshake.
	 */
	public function fromResponse(array $params): ?IWebSocketExtension
	{
		if (!self::isWellFormed($params)) {
			return null;
		}
		$serverNoTakeover = isset($params[self::PARAM_SERVER_NO_CONTEXT_TAKEOVER]);
		if ($this->_serverNoContextTakeover && !$serverNoTakeover) {
			return null;   // §7.1.1.1: the server accepts an offered server_no_context_takeover only by echoing it
		}
		$clientNoTakeover = $this->_clientNoContextTakeover || isset($params[self::PARAM_CLIENT_NO_CONTEXT_TAKEOVER]);

		$deflateWindow = $this->_clientMaxWindowBits ?? TPermessageDeflateExtension::MAX_WINDOW_BITS;
		if (array_key_exists(self::PARAM_CLIENT_MAX_WINDOW_BITS, $params)) {
			if ($this->_clientMaxWindowBits === null) {
				return null;   // §7.1.2.2: the response may carry client_max_window_bits only when the offer did
			}
			$bits = self::parseWindowBits($params[self::PARAM_CLIENT_MAX_WINDOW_BITS]);   // a bare or invalid value fails
			if ($bits === null || $bits < TPermessageDeflateExtension::MIN_WINDOW_BITS) {
				return null;   // raw DEFLATE cannot compress within an 8-bit window
			}
			$deflateWindow = min($deflateWindow, $bits);
		}
		if (array_key_exists(self::PARAM_SERVER_MAX_WINDOW_BITS, $params)) {
			$bits = self::parseWindowBits($params[self::PARAM_SERVER_MAX_WINDOW_BITS]);
			if ($bits === null || ($this->_serverMaxWindowBits !== null && $bits > $this->_serverMaxWindowBits)) {
				return null;   // §7.1.2.1: the server may not widen the window the client requested
			}
		}
		return new TPermessageDeflateExtension($deflateWindow, $clientNoTakeover, $serverNoTakeover, $this->_level);
	}

	/**
	 * Indicates whether a parameter set uses only the RFC 7692 parameters, with the two takeover
	 * flags carrying no value.
	 * @param array<string, bool|string> $params The parameters.
	 * @return bool Whether the parameters are well formed.
	 */
	private static function isWellFormed(array $params): bool
	{
		foreach ($params as $name => $value) {
			if (!in_array($name, self::KNOWN_PARAMS, true)) {
				return false;
			}
			if ($value !== true && in_array($name, self::FLAG_PARAMS, true)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Parses a `*_max_window_bits` value.  Only a decimal integer token from 8 to 15 is valid; a bare
	 * parameter, `10x`, `7`, and `16` are not.
	 * @param bool|string $value The parameter value (`true` when bare).
	 * @return ?int The window bits, or null when the value is invalid.
	 */
	private static function parseWindowBits(bool|string $value): ?int
	{
		if (!is_string($value) || preg_match('/^\d{1,2}$/', $value) !== 1) {
			return null;
		}
		$bits = (int) $value;
		return ($bits >= self::PARAM_MIN_WINDOW_BITS && $bits <= TPermessageDeflateExtension::MAX_WINDOW_BITS) ? $bits : null;
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
