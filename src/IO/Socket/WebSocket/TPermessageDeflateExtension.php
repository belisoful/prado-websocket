<?php

/**
 * TPermessageDeflateExtension class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\Exceptions\TInvalidDataValueException;
use Prado\TComponent;

/**
 * TPermessageDeflateExtension class.
 *
 * The RFC 7692 `permessage-deflate` extension, which compresses each data message with DEFLATE and
 * marks the compressed message with {@see IWebSocketExtension::RSV1}.  {@see TWebSocketConnection}
 * runs it as the message-pipeline transform: {@see encodeMessage()} compresses on send,
 * {@see decodeMessage()} decompresses on receive.
 *
 * Compression uses a raw DEFLATE context with a per-message sync flush, then removes the trailing
 * empty block (`0x00 0x00 0xFF 0xFF`) the flush appends (RFC 7692 §7.2.1); decompression restores
 * the trailer before inflating (§7.2.2).  An empty message compresses to a single empty block
 * (`0x00`).  A message whose DEFLATE data stops short of a block boundary is refused as invalid
 * payload (close code 1007), never delivered in part; a message that ends its DEFLATE stream with a
 * `BFINAL` block (§7.2.3.4) is accepted and the next message begins a new stream.
 *
 * The context is reused across messages by default, so a message's DEFLATE dictionary carries the
 * history of the ones before it (context takeover).  {@see getDeflateNoContextTakeover()} and
 * {@see getInflateNoContextTakeover()} reset the relevant context after every message, trading the
 * cross-message ratio for the smaller memory footprint RFC 7692 negotiates through the
 * `*_no_context_takeover` parameters.  {@see getDeflateWindowBits()} bounds the send-side LZ77
 * window; the receive side always inflates with the maximum window, which decodes any peer window.
 *
 * A {@see TPermessageDeflateNegotiator} configures and produces this extension during the handshake.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc7692.html
 */
class TPermessageDeflateExtension extends TComponent implements IWebSocketExtension
{
	/** The extension token as it appears in the negotiation header. */
	public const NAME = 'permessage-deflate';

	/** The smallest LZ77 window raw DEFLATE supports; zlib rejects 8. */
	public const MIN_WINDOW_BITS = 9;

	/** The largest LZ77 window, which inflates any compressed window. */
	public const MAX_WINDOW_BITS = 15;

	/** The lowest DEFLATE compression level; -1 selects the zlib default. */
	public const MIN_LEVEL = -1;

	/** The highest DEFLATE compression level. */
	public const MAX_LEVEL = 9;

	/** The empty DEFLATE block a sync flush appends, removed on send and restored on receive. */
	private const FLUSH_TRAILER = "\x00\x00\xff\xff";

	/** The single empty non-final block that represents an empty compressed message. */
	private const EMPTY_BLOCK = "\x00";

	/** A whole empty stored block, fed after the trailer to prove the stream sits at a block boundary. */
	private const BOUNDARY_PROBE = "\x00\x00\x00\xff\xff";

	/** The largest compressed-input chunk fed to inflate per step. */
	private const INFLATE_CHUNK = 8192;

	/** The most output one byte of DEFLATE input can expand to (zlib's bound), which sizes each inflate step against the remaining output budget. */
	public const MAX_INFLATE_RATIO = 1032;

	/** @var int The send-side LZ77 window, in bits (9-15). */
	private int $_deflateWindowBits;

	/** @var bool Whether the send-side context resets after each message. */
	private bool $_deflateNoContextTakeover;

	/** @var bool Whether the receive-side context resets after each message. */
	private bool $_inflateNoContextTakeover;

	/** @var int The DEFLATE compression level (-1 for the zlib default, 0-9 otherwise). */
	private int $_level;

	/** @var int The maximum decompressed message size in bytes, or 0 for unlimited. */
	private int $_maxOutputLength = 0;

	/** @var ?\DeflateContext The send-side context, created on first use. */
	private ?\DeflateContext $_deflate = null;

	/** @var ?\InflateContext The receive-side context, created on first use. */
	private ?\InflateContext $_inflate = null;

	/**
	 * @param int $deflateWindowBits The send-side LZ77 window, clamped to 9-15.
	 * @param bool $deflateNoContextTakeover Whether to reset the send-side context after each message.
	 * @param bool $inflateNoContextTakeover Whether to reset the receive-side context after each message.
	 * @param int $level The DEFLATE compression level (-1 for the zlib default, 0-9 otherwise).
	 * @throws TInvalidDataValueException When the level is not -1 or 0-9.
	 */
	public function __construct(int $deflateWindowBits = self::MAX_WINDOW_BITS, bool $deflateNoContextTakeover = false, bool $inflateNoContextTakeover = false, int $level = -1)
	{
		$this->_deflateWindowBits = max(self::MIN_WINDOW_BITS, min(self::MAX_WINDOW_BITS, $deflateWindowBits));
		$this->_deflateNoContextTakeover = $deflateNoContextTakeover;
		$this->_inflateNoContextTakeover = $inflateNoContextTakeover;
		$this->_level = self::ensureLevel($level);
		parent::__construct();
	}

	/**
	 * Validates a DEFLATE compression level.
	 * @param int $level The level to check.
	 * @throws TInvalidDataValueException When the level is not -1 or 0-9.
	 * @return int The level.
	 */
	public static function ensureLevel(int $level): int
	{
		if ($level < self::MIN_LEVEL || $level > self::MAX_LEVEL) {
			throw new TInvalidDataValueException('websocket_permessage_deflate_level_invalid', $level);
		}
		return $level;
	}

	/**
	 * Returns the extension token, `permessage-deflate`.
	 * @return string The extension name.
	 */
	public function getName(): string
	{
		return self::NAME;
	}

	/**
	 * Returns the reserved bit the extension owns, {@see IWebSocketExtension::RSV1}.
	 * @return int The reserved-bit mask.
	 */
	public function getReservedRsv(): int
	{
		return IWebSocketExtension::RSV1;
	}

	/**
	 * Compresses an outgoing message and reports that it set {@see IWebSocketExtension::RSV1}.  The
	 * sync-flush trailer is removed; an empty message becomes a single empty block.
	 * @param string $payload The message payload.
	 * @throws TWebSocketException When the DEFLATE context cannot be created.
	 * @return array{0: string, 1: int} The compressed payload and the RSV1 bit.
	 */
	public function encodeMessage(string $payload): array
	{
		$out = deflate_add($this->deflateContext(), $payload, ZLIB_SYNC_FLUSH);
		if (str_ends_with($out, self::FLUSH_TRAILER)) {
			$out = substr($out, 0, -4);
		}
		if ($out === '') {
			$out = self::EMPTY_BLOCK;
		}
		if ($this->_deflateNoContextTakeover) {
			$this->_deflate = null;
		}
		return [$out, IWebSocketExtension::RSV1];
	}

	/**
	 * Decompresses a received message when its first frame set {@see IWebSocketExtension::RSV1}; an
	 * uncompressed message passes through.
	 *
	 * The compressed input is fed in chunks sized by {@see inflateChunkSize()} against the remaining
	 * output budget, so a decompression bomb is aborted once the output passes
	 * {@see getMaxOutputLength() the limit} and materializes at most {@see MAX_INFLATE_RATIO} bytes
	 * beyond it.  A `BFINAL` block ends the sender's stream; the bytes behind it begin a new one.
	 *
	 * The sync-flush trailer is then restored (§7.2.2), which completes the empty stored block a
	 * conforming sender ends on, and a further empty stored block follows it.  A complete stream
	 * inflates both to nothing; output from either means the sender's data was cut short of a block
	 * boundary, and the message is refused rather than delivered in part.
	 * @param string $payload The message payload.
	 * @param int $rsv The reserved bits set on the message's first frame.
	 * @throws TWebSocketException When the compressed data is corrupt or truncated (close code 1007),
	 *   or its output exceeds the limit (close code 1009).
	 * @return string The decompressed payload.
	 */
	public function decodeMessage(string $payload, int $rsv): string
	{
		if (($rsv & IWebSocketExtension::RSV1) === 0) {
			return $payload;
		}
		$context = $this->inflateContext();
		$length = strlen($payload);
		$out = '';
		for ($offset = 0; $offset < $length;) {
			$step = substr($payload, $offset, $this->inflateChunkSize(strlen($out)));
			$consumedBefore = inflate_get_read_len($context);
			$out .= $this->inflate($context, $step);
			if ($this->_maxOutputLength > 0 && strlen($out) > $this->_maxOutputLength) {
				throw (new TWebSocketException('websocket_message_too_big', $this->_maxOutputLength))
					->setCloseCode(TWebSocketCloseCode::MessageTooBig);
			}
			if (inflate_get_status($context) === ZLIB_STREAM_END) {
				$offset += max(1, inflate_get_read_len($context) - $consumedBefore);   // resume behind the BFINAL block on a new stream
				$this->_inflate = null;
				$context = $this->inflateContext();
			} else {
				$offset += strlen($step);
			}
		}
		if ($this->inflate($context, self::FLUSH_TRAILER) !== '' || $this->inflate($context, self::BOUNDARY_PROBE) !== '') {
			throw (new TWebSocketException('websocket_permessage_deflate_inflate_failed'))
				->setCloseCode(TWebSocketCloseCode::InvalidFramePayload);   // the data ended inside a block
		}
		if ($this->_inflateNoContextTakeover) {
			$this->_inflate = null;
		}
		return $out;
	}

	/**
	 * Runs one inflate step with a sync flush.
	 * @param \InflateContext $context The receive-side context.
	 * @param string $input The compressed bytes to feed.
	 * @throws TWebSocketException When zlib reports a data error (close code 1007).
	 * @return string The inflated bytes.
	 */
	private function inflate(\InflateContext $context, string $input): string
	{
		$out = @inflate_add($context, $input, ZLIB_SYNC_FLUSH);   // a data error returns false with a warning
		if ($out === false) {
			throw (new TWebSocketException('websocket_permessage_deflate_inflate_failed'))
				->setCloseCode(TWebSocketCloseCode::InvalidFramePayload);
		}
		return $out;
	}

	/**
	 * Returns how many compressed bytes the next inflate step may consume so that its output cannot
	 * overshoot {@see getMaxOutputLength() the limit} by more than one input byte's expansion.  Each
	 * DEFLATE input byte expands to at most {@see MAX_INFLATE_RATIO} output bytes, so the step is the
	 * remaining budget divided by that ratio (at least one byte, at most {@see INFLATE_CHUNK}); an
	 * unlimited output uses the full chunk.
	 * @param int $produced The output bytes produced so far.
	 * @return int The compressed bytes to feed next.
	 */
	protected function inflateChunkSize(int $produced): int
	{
		if ($this->_maxOutputLength <= 0) {
			return self::INFLATE_CHUNK;
		}
		$budget = max(0, $this->_maxOutputLength - $produced);
		return max(1, min(self::INFLATE_CHUNK, intdiv($budget, self::MAX_INFLATE_RATIO) + 1));
	}

	/**
	 * Returns the maximum decompressed message size.
	 * @return int The maximum decompressed size in bytes, or 0 for unlimited.
	 */
	public function getMaxOutputLength(): int
	{
		return $this->_maxOutputLength;
	}

	/**
	 * Sets the maximum decompressed message size.  A {@see TWebSocketConnection} propagates its own
	 * message-size limit here so the inflate is bounded to the same value.
	 * @param int $value The maximum decompressed size in bytes, or 0 for unlimited.
	 */
	public function setMaxOutputLength(int $value): void
	{
		$this->_maxOutputLength = max(0, $value);
	}

	/**
	 * Returns the send-side LZ77 window, in bits.
	 * @return int The window bits (9-15).
	 */
	public function getDeflateWindowBits(): int
	{
		return $this->_deflateWindowBits;
	}

	/**
	 * Returns whether the send-side context resets after each message.
	 * @return bool Whether send-side context takeover is off.
	 */
	public function getDeflateNoContextTakeover(): bool
	{
		return $this->_deflateNoContextTakeover;
	}

	/**
	 * Returns whether the receive-side context resets after each message.
	 * @return bool Whether receive-side context takeover is off.
	 */
	public function getInflateNoContextTakeover(): bool
	{
		return $this->_inflateNoContextTakeover;
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
	 * Returns the send-side DEFLATE context, creating it on first use.
	 * @throws TWebSocketException When zlib refuses the parameters or cannot allocate the context.
	 * @return \DeflateContext The DEFLATE context.
	 */
	private function deflateContext(): \DeflateContext
	{
		if ($this->_deflate === null) {
			try {
				$context = deflate_init(ZLIB_ENCODING_RAW, ['level' => $this->_level, 'window' => $this->_deflateWindowBits]);
			} catch (\ValueError $e) {
				$context = false;   // PHP 8 rejects an out-of-range option by throwing
			}
			if ($context === false) {
				throw new TWebSocketException('websocket_permessage_deflate_init_failed');
			}
			$this->_deflate = $context;
		}
		return $this->_deflate;
	}

	/**
	 * Returns the receive-side INFLATE context, creating it on first use.  The maximum window inflates
	 * any window the peer compressed with.
	 * @throws TWebSocketException When zlib refuses the parameters or cannot allocate the context.
	 * @return \InflateContext The INFLATE context.
	 */
	private function inflateContext(): \InflateContext
	{
		if ($this->_inflate === null) {
			try {
				$context = inflate_init(ZLIB_ENCODING_RAW, ['window' => self::MAX_WINDOW_BITS]);
			} catch (\ValueError $e) {
				$context = false;
			}
			if ($context === false) {
				throw new TWebSocketException('websocket_permessage_deflate_init_failed');
			}
			$this->_inflate = $context;
		}
		return $this->_inflate;
	}
}
