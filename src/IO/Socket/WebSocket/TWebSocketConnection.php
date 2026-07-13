<?php

/**
 * TWebSocketConnection class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\IO\TResource;
use Prado\Prado;
use Prado\TComponent;
use Psr\Http\Message\StreamInterface;

/**
 * TWebSocketConnection class.
 *
 * Wraps a transport stream (typically a {@see \Prado\IO\Socket\TSocketStream}) as an RFC 6455
 * connection: {@see send()}/{@see sendBinary()} write data messages, {@see receive()} returns
 * the next complete message (reassembling fragments), and {@see ping()}/{@see pong()}/
 * {@see close()} drive the control flow.  A client connection masks every frame; a server
 * connection does not.  {@see accept()} and {@see connect()} build a connection after running
 * the {@see TWebSocketHandshake}.
 *
 * {@see receive()} handles control frames inline: a Ping is auto-answered with a Pong, a Close
 * completes the close handshake (echoing a Close when the peer initiated), and a clean end of
 * stream returns null.  Use {@see getLastOpcode()} to tell a text message from a binary one.
 *
 * {@see receive()} blocks for the next message; {@see feed()} is its non-blocking counterpart for
 * a {@see TWebSocketServer} event loop, taking the bytes just read and returning the complete
 * messages they yield.
 *
 * Events ('on' prefix).  Each is a real method taking a single mixed $param:
 *  - onPing: raised with the Ping payload (a Pong is sent automatically afterward).
 *  - onPong: raised with the Pong payload.
 *  - onClose: raised with the Close {@see TWebSocketFrame} when the peer closes.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc6455.html
 */
class TWebSocketConnection extends TComponent
{
	/** The default maximum reassembled message size in bytes (10 MiB); bounds memory by default. */
	public const DEFAULT_MAX_MESSAGE_SIZE = 10 * 1024 * 1024;

	/** The default maximum queued outbound bytes before a slow reader is dropped (16 MiB). */
	public const DEFAULT_MAX_SEND_BUFFER = 16 * 1024 * 1024;

	/** @var StreamInterface The transport stream. */
	private StreamInterface $_stream;

	/** @var bool Whether this is the client side (which masks outgoing frames). */
	private bool $_isClient;

	/** @var bool Whether a Close frame has been sent. */
	private bool $_closing = false;

	/** @var bool Whether the connection is closed (Close exchanged or stream ended). */
	private bool $_closed = false;

	/** @var ?int The opcode of the most recently received data message (Text or Binary). */
	private ?int $_lastOpcode = null;

	/** @var string Unparsed bytes carried between {@see feed()} calls. */
	private string $_readBuffer = '';

	/** @var string Reassembly buffer for a fragmented message across {@see feed()} calls. */
	private string $_messageBuffer = '';

	/** @var ?int The opcode (Text or Binary) of the message being reassembled, or null when none is. */
	private ?int $_fragmentOpcode = null;

	/** @var int The reserved bits of the first frame of the message being reassembled. */
	private int $_fragmentRsv = 0;

	/** @var bool Whether to enforce the RFC 6455 mask rule on received frames. */
	private bool $_validateMasking = true;

	/** @var int The maximum reassembled message size in bytes, or 0 for unlimited. */
	private int $_maxMessageSize = self::DEFAULT_MAX_MESSAGE_SIZE;

	/** @var string Bytes queued for the wire that a non-blocking socket has not yet accepted. */
	private string $_outbound = '';

	/** @var int The maximum queued outbound bytes before a slow reader is dropped, or 0 for unlimited. */
	private int $_maxSendBufferBytes = self::DEFAULT_MAX_SEND_BUFFER;

	/** @var ?string The negotiated subprotocol, or null when none was selected. */
	private ?string $_subprotocol = null;

	/** @var IWebSocketExtension[] The negotiated extensions, applied in order on send and reverse on receive. */
	private array $_extensions = [];

	/**
	 * @param StreamInterface $stream The transport stream.
	 * @param bool $isClient Whether this is the client side. Default false (server).
	 */
	public function __construct(StreamInterface $stream, bool $isClient = false)
	{
		$this->_stream = $stream;
		$this->_isClient = $isClient;
		parent::__construct();
	}

	/**
	 * Accepts a server connection: runs the server handshake on the stream, then wraps it with the
	 * negotiated subprotocol and extension applied.
	 * @param StreamInterface $stream The accepted transport stream.
	 * @param array{subprotocols?: string[], extensions?: IWebSocketExtensionNegotiator[], origins?: string[], headers?: array<string, string>} $options
	 *   The handshake negotiation options, including the allowed `origins`.
	 * @return self The server-side connection.
	 */
	public static function accept(StreamInterface $stream, array $options = []): self
	{
		$result = TWebSocketHandshake::acceptConnection($stream, $options);
		return self::wrapNegotiated($stream, false, $result);
	}

	/**
	 * Opens a client connection: runs the client handshake on the stream, then wraps it with the
	 * selected subprotocol and extension applied.
	 * @param StreamInterface $stream The connected transport stream.
	 * @param string $host The Host header value.
	 * @param string $path The request target. Default '/'.
	 * @param array{subprotocols?: string[], extensions?: IWebSocketExtensionNegotiator[], headers?: array<string, string>} $options
	 *   The handshake negotiation options.
	 * @return self The client-side connection.
	 */
	public static function connect(StreamInterface $stream, string $host, string $path = '/', array $options = []): self
	{
		$result = TWebSocketHandshake::openConnection($stream, $host, $path, $options);
		return self::wrapNegotiated($stream, true, $result);
	}

	/**
	 * Opens a client connection from a `ws://`/`wss://` URL: parses the host and request target with
	 * {@see TWebSocketHandshake::parseUrl()} and runs the client handshake on the given transport.
	 * The transport must already match the URL's scheme (a TLS stream for `wss://`).
	 * @param StreamInterface $stream The connected transport stream.
	 * @param string $url The `ws://` or `wss://` URL.
	 * @param array{subprotocols?: string[], extensions?: IWebSocketExtensionNegotiator[], headers?: array<string, string>} $options
	 *   The handshake negotiation options.
	 * @return self The client-side connection.
	 */
	public static function connectUrl(StreamInterface $stream, string $url, array $options = []): self
	{
		$parts = TWebSocketHandshake::parseUrl($url);
		return self::connect($stream, $parts['hostHeader'], $parts['path'], $options);
	}

	/**
	 * Builds a connection from a handshake result, applying the negotiated subprotocol.
	 * @param StreamInterface $stream The transport stream.
	 * @param bool $isClient Whether this is the client side.
	 * @param array{subprotocol?: ?string} $result The handshake result.
	 * @return self The configured connection.
	 */
	private static function wrapNegotiated(StreamInterface $stream, bool $isClient, array $result): self
	{
		$connection = Prado::createComponent(self::class, $stream, $isClient);
		$connection->setSubprotocol($result['subprotocol'] ?? null);
		$connection->setExtensions($result['extensions'] ?? []);
		return $connection;
	}

	/** @return StreamInterface The transport stream. */
	public function getStream(): StreamInterface
	{
		return $this->_stream;
	}

	/** @return bool Whether this is the client side. */
	public function getIsClient(): bool
	{
		return $this->_isClient;
	}

	/** @return bool Whether a Close frame has been sent. */
	public function getIsClosing(): bool
	{
		return $this->_closing;
	}

	/** @return bool Whether the connection is closed. */
	public function getIsClosed(): bool
	{
		return $this->_closed;
	}

	/**
	 * Returns the opcode of the most recently received data message, the right opcode after a
	 * single {@see receive()}.  A per-message handler over {@see feedMessages()} reads each
	 * message's own {@see TWebSocketMessage::getOpcode()} instead, as a batch leaves this holding
	 * only the last message's opcode.
	 * @return ?int A {@see TWebSocketOpcode} value, or null before any data message.
	 */
	public function getLastOpcode(): ?int
	{
		return $this->_lastOpcode;
	}

	/**
	 * Encodes a frame, masking it on the client side, and queues it for the wire.
	 * @param TWebSocketFrame $frame The frame to send.
	 * @return int The number of bytes queued.
	 */
	public function sendFrame(TWebSocketFrame $frame): int
	{
		$maskKey = $this->_isClient ? random_bytes(4) : null;
		return $this->writeAll(TWebSocketFrameCodec::encode($frame, $maskKey));
	}

	/**
	 * Queues bytes for the wire and drains what the socket accepts now, without blocking.  On a
	 * non-blocking socket whose send buffer is full, the unwritten tail stays queued for the event
	 * loop to flush once the socket is {@see hasPendingOutbound() writable}; on a blocking socket the
	 * write completes here.  A reader too slow to drain the queue past {@see getMaxSendBufferBytes()}
	 * is dropped rather than allowed to grow the buffer without bound.
	 * @param string $data The bytes to queue.
	 * @throws TWebSocketException When the queued backlog exceeds the send-buffer limit, or the write fails.
	 * @return int The number of bytes queued.
	 */
	private function writeAll(string $data): int
	{
		$this->_outbound .= $data;
		$this->flushOutbound();
		if ($this->_maxSendBufferBytes > 0 && strlen($this->_outbound) > $this->_maxSendBufferBytes) {
			throw (new TWebSocketException('websocket_send_buffer_overflow', $this->_maxSendBufferBytes))
				->setCloseCode(TWebSocketCloseCode::GoingAway);
		}
		return strlen($data);
	}

	/**
	 * Drains the queued outbound bytes to the socket without blocking, keeping any tail the socket
	 * could not accept.  The event loop calls this when the transport becomes writable; a blocking
	 * socket drains fully in one call.
	 * @throws TWebSocketException When the underlying write fails (a broken pipe).
	 * @return int The number of bytes flushed on this call.
	 */
	public function flushOutbound(): int
	{
		if ($this->_outbound === '') {
			return 0;
		}
		$length = strlen($this->_outbound);
		$sent = 0;
		try {
			while ($sent < $length) {
				$wrote = $this->_stream->write($sent === 0 ? $this->_outbound : substr($this->_outbound, $sent));
				if ($wrote <= 0) {
					break;   // the non-blocking send buffer is full; keep the tail for the next writable event
				}
				$sent += $wrote;
			}
		} catch (\RuntimeException $e) {
			$this->_outbound = '';
			$this->_closed = true;
			throw (new TWebSocketException('websocket_write_failed'))->setCloseCode(TWebSocketCloseCode::GoingAway);
		}
		$this->_outbound = $sent >= $length ? '' : substr($this->_outbound, $sent);
		return $sent;
	}

	/** @return bool Whether bytes are queued for the wire awaiting a writable socket. */
	public function hasPendingOutbound(): bool
	{
		return $this->_outbound !== '';
	}

	/** @return int The number of bytes queued for the wire. */
	public function getPendingOutboundLength(): int
	{
		return strlen($this->_outbound);
	}

	/**
	 * Sends a Text message.  A no-op once a Close has been sent, as a data frame must not follow it.
	 * @param string $text The UTF-8 text.
	 * @return int The bytes written, or 0 when the connection is closing.
	 */
	public function send(string $text): int
	{
		return $this->sendDataMessage(TWebSocketOpcode::Text, $text);
	}

	/**
	 * Sends a Binary message.  A no-op once a Close has been sent, as a data frame must not follow it.
	 * @param string $data The bytes.
	 * @return int The bytes written, or 0 when the connection is closing.
	 */
	public function sendBinary(string $data): int
	{
		return $this->sendDataMessage(TWebSocketOpcode::Binary, $data);
	}

	/**
	 * Sends a data message as a single frame, folding the extension pipeline over the payload in
	 * order and setting the reserved bits the extensions report.  A no-op once a Close has been sent.
	 * @param int $opcode The data opcode (Text or Binary).
	 * @param string $payload The message payload.
	 * @return int The bytes written, or 0 when the connection is closing.
	 */
	private function sendDataMessage(int $opcode, string $payload): int
	{
		if ($this->_closing) {
			return 0;
		}
		$rsv = 0;
		foreach ($this->_extensions as $extension) {
			[$payload, $bits] = $extension->encodeMessage($payload);
			$rsv |= $bits;
		}
		return $this->sendFrame(new TWebSocketFrame(
			$opcode,
			$payload,
			true,
			($rsv & IWebSocketExtension::RSV1) !== 0,
			($rsv & IWebSocketExtension::RSV2) !== 0,
			($rsv & IWebSocketExtension::RSV3) !== 0,
		));
	}

	/** Sends a Ping. @param string $data The application data (<=125 bytes). @return int Bytes written. */
	public function ping(string $data = ''): int
	{
		return $this->sendFrame(TWebSocketFrame::ping($data));
	}

	/** Sends a Pong. @param string $data The application data (<=125 bytes). @return int Bytes written. */
	public function pong(string $data = ''): int
	{
		return $this->sendFrame(TWebSocketFrame::pong($data));
	}

	/**
	 * Sends a Close frame, beginning the close handshake.  A second call is a no-op.
	 * @param int $code A {@see TWebSocketCloseCode} value. Default Normal.
	 * @param string $reason A reason phrase. Default ''.
	 */
	public function close(int $code = TWebSocketCloseCode::Normal, string $reason = ''): void
	{
		if ($this->_closing) {
			return;
		}
		$this->_closing = true;
		$this->sendFrame(TWebSocketFrame::close($code, $reason));
	}

	/**
	 * Completes the close handshake on a blocking connection: sends a Close (when not already closing),
	 * then reads and discards frames until the peer's Close arrives or the stream ends.  When the
	 * transport supports it, a read timeout bounds the wait so a peer that never answers cannot block
	 * the caller forever.
	 * @param int $code A {@see TWebSocketCloseCode} value. Default Normal.
	 * @param string $reason A reason phrase. Default ''.
	 * @param ?float $timeout The seconds to wait for the peer's Close, or null for an unbounded wait.
	 * @return bool Whether the connection closed (the peer's Close or end of stream) before the timeout.
	 */
	public function drainClose(int $code = TWebSocketCloseCode::Normal, string $reason = '', ?float $timeout = 1.0): bool
	{
		try {
			$this->close($code, $reason);
			if ($timeout !== null && $this->_stream instanceof TResource) {
				$seconds = (int) $timeout;
				$this->_stream->setTimeout($seconds, (int) (($timeout - $seconds) * 1000000));
			}
			while (!$this->_closed && $this->receiveFrame() !== null) {
				// receiveFrame() handles the peer's Close inline, marking the connection closed.
			}
		} catch (\Throwable $e) {
			// The peer is gone or unresponsive (a broken pipe, a read timeout, a protocol error);
			// the drain ends and the connection is considered closed regardless.
			$this->_closed = true;
		}
		return $this->_closed;
	}

	/**
	 * Reads and returns the next single frame, handling control frames inline (auto-Pong,
	 * close handshake).  Useful for an event loop that pumps one frame at a time; the
	 * message-level {@see receive()} builds on it.
	 * @return ?TWebSocketFrame The frame, or null at a clean end of stream.
	 */
	public function receiveFrame(): ?TWebSocketFrame
	{
		$frame = TWebSocketFrameCodec::decode($this->_stream, $this->expectedMask(), $this->_maxMessageSize);
		if ($frame === null) {
			$this->_closed = true;
			return null;
		}
		$this->validateFrame($frame);
		if ($frame->getIsControl()) {
			$this->handleControlFrame($frame);
		}
		return $frame;
	}

	/**
	 * Reads the next complete data message, reassembling fragments and handling control frames
	 * transparently (a lone Ping is answered and the wait continues for the next data message).
	 * @return ?string The message payload, or null when the connection closes or the stream ends.
	 */
	public function receive(): ?string
	{
		while (!$this->_closed) {
			$frame = $this->receiveFrame();
			if ($frame === null || $frame->getOpcode() === TWebSocketOpcode::Close) {
				return null;
			}
			if ($frame->getIsControl()) {
				continue;
			}
			$message = $this->ingestDataFrame($frame);
			if ($message !== null) {
				return $message;
			}
		}
		return null;
	}

	/**
	 * Feeds received bytes and returns the complete data messages they yield (non-blocking), each
	 * paired with its opcode.
	 *
	 * This is the event-loop counterpart to {@see receive()}: a {@see select()}-driven server
	 * reads whatever bytes are available and feeds them here without blocking.  Bytes are
	 * buffered across calls, every complete frame is extracted, control frames are handled inline
	 * (auto-Pong, close handshake), and fragmented messages are reassembled.  A Close frame marks
	 * the connection {@see getIsClosed() closed}.
	 *
	 * One read can yield several messages of different kinds, so each {@see TWebSocketMessage} carries
	 * its own opcode; a per-message handler reads that rather than {@see getLastOpcode()}, which holds
	 * only the last message's opcode after the batch.
	 *
	 * @param string $bytes The bytes just read from the transport.
	 * @throws TWebSocketException When a frame is malformed (the caller should close).
	 * @return TWebSocketMessage[] The complete messages, in order (empty when none completed).
	 */
	public function feedMessages(string $bytes): array
	{
		$this->_readBuffer .= $bytes;
		$messages = [];
		while (!$this->_closed && ($decoded = TWebSocketFrameCodec::tryDecode($this->_readBuffer, $this->expectedMask(), $this->_maxMessageSize)) !== null) {
			$this->_readBuffer = substr($this->_readBuffer, $decoded['length']);
			$frame = $decoded['frame'];
			$this->validateFrame($frame);
			if ($frame->getIsControl()) {
				$this->handleControlFrame($frame);
				continue;
			}
			$message = $this->ingestDataFrame($frame);
			if ($message !== null) {
				// _lastOpcode holds this message's opcode here: it was set on the message's first
				// frame and is not overwritten until the next message begins.
				$messages[] = new TWebSocketMessage($this->_lastOpcode ?? TWebSocketOpcode::Text, $message);
			}
		}
		return $messages;
	}

	/**
	 * Feeds received bytes and returns the complete data message payloads they yield (non-blocking).
	 * The string-only counterpart to {@see feedMessages()}, for a caller that does not need the
	 * per-message opcode (e.g. an envelope reader).
	 * @param string $bytes The bytes just read from the transport.
	 * @throws TWebSocketException When a frame is malformed (the caller should close).
	 * @return string[] The complete message payloads, in order (empty when none completed).
	 */
	public function feed(string $bytes): array
	{
		return array_map(static fn (TWebSocketMessage $message): string => $message->getPayload(), $this->feedMessages($bytes));
	}

	/**
	 * Handles a control frame: auto-Pong a Ping, raise the event, and complete the close
	 * handshake on a Close (echoing a Close when the peer initiated it).
	 * @param TWebSocketFrame $frame The control frame.
	 */
	private function handleControlFrame(TWebSocketFrame $frame): void
	{
		switch ($frame->getOpcode()) {
			case TWebSocketOpcode::Ping:
				$this->onPing($frame->getPayload());
				$this->sendFrame(TWebSocketFrame::pong($frame->getPayload()));
				break;
			case TWebSocketOpcode::Pong:
				$this->onPong($frame->getPayload());
				break;
			case TWebSocketOpcode::Close:
				$this->validateCloseFrame($frame);
				$this->onClose($frame);
				if (!$this->_closing) {
					$this->_closing = true;
					$code = $frame->getCloseCode();
					$echo = ($code !== null && TWebSocketCloseCode::isSendable($code)) ? $code : TWebSocketCloseCode::Normal;
					$this->sendFrame(TWebSocketFrame::close($echo));
				}
				$this->_closed = true;
				break;
		}
	}

	/**
	 * Returns the mask state received frames must have, or null when masking is not enforced.  A
	 * server requires masked frames and a client requires unmasked frames (RFC 6455 section 5.1).
	 * @return ?bool True to require masked, false to require unmasked, or null to skip the check.
	 */
	private function expectedMask(): ?bool
	{
		return $this->_validateMasking ? !$this->_isClient : null;
	}

	/**
	 * Validates a frame's stateless structure: a reserved bit no negotiated extension owns, or an
	 * undefined opcode, is a protocol error.  A reserved bit is allowed only on a data message's
	 * first frame, and only when an extension reserves it.
	 * @param TWebSocketFrame $frame The decoded frame.
	 * @throws TWebSocketException When an unowned reserved bit is set or the opcode is undefined.
	 */
	private function validateFrame(TWebSocketFrame $frame): void
	{
		$allowed = (!$frame->getIsControl() && $frame->getOpcode() !== TWebSocketOpcode::Continuation)
			? $this->reservedRsv()
			: 0;
		if ((self::frameRsv($frame) & ~$allowed) !== 0) {
			throw new TWebSocketException('websocket_rsv_not_negotiated');
		}
		if (!TWebSocketOpcode::isDefined($frame->getOpcode())) {
			throw new TWebSocketException('websocket_opcode_unknown', $frame->getOpcode());
		}
	}

	/**
	 * Returns the reserved bits owned by the negotiated extensions, the union of their masks.
	 * @return int The reserved-bit mask owned by extensions.
	 */
	private function reservedRsv(): int
	{
		$reserved = 0;
		foreach ($this->_extensions as $extension) {
			$reserved |= $extension->getReservedRsv();
		}
		return $reserved;
	}

	/**
	 * Returns a frame's reserved bits as an {@see IWebSocketExtension} RSV mask.
	 * @param TWebSocketFrame $frame The frame.
	 * @return int The reserved-bit mask.
	 */
	private static function frameRsv(TWebSocketFrame $frame): int
	{
		return ($frame->getRsv1() ? IWebSocketExtension::RSV1 : 0)
			| ($frame->getRsv2() ? IWebSocketExtension::RSV2 : 0)
			| ($frame->getRsv3() ? IWebSocketExtension::RSV3 : 0);
	}

	/**
	 * Reassembles a data frame into a message, enforcing the fragmentation rules: a continuation
	 * with no message in progress, or a new Text/Binary frame while one is unfinished, is a protocol
	 * error.  On the final frame it enforces the size limit and validates UTF-8 for a Text message.
	 * @param TWebSocketFrame $frame The data frame (Text, Binary, or Continuation).
	 * @throws TWebSocketException On a fragmentation error, an oversized message, or invalid UTF-8.
	 * @return ?string The completed message, or null while the message is still being reassembled.
	 */
	private function ingestDataFrame(TWebSocketFrame $frame): ?string
	{
		$opcode = $frame->getOpcode();
		if ($opcode === TWebSocketOpcode::Continuation) {
			if ($this->_fragmentOpcode === null) {
				throw new TWebSocketException('websocket_continuation_unexpected');
			}
		} else {
			if ($this->_fragmentOpcode !== null) {
				throw new TWebSocketException('websocket_fragment_incomplete');
			}
			$this->_fragmentOpcode = $opcode;
			$this->_fragmentRsv = self::frameRsv($frame);
			$this->_lastOpcode = $opcode;
		}
		$this->_messageBuffer .= $frame->getPayload();
		if ($this->_maxMessageSize > 0 && strlen($this->_messageBuffer) > $this->_maxMessageSize) {
			$this->resetMessage();
			throw (new TWebSocketException('websocket_message_too_big', $this->_maxMessageSize))
				->setCloseCode(TWebSocketCloseCode::MessageTooBig);
		}
		if (!$frame->getFin()) {
			return null;
		}
		$message = $this->_messageBuffer;
		$isText = $this->_fragmentOpcode === TWebSocketOpcode::Text;
		$rsv = $this->_fragmentRsv;
		$this->resetMessage();
		foreach (array_reverse($this->_extensions) as $extension) {
			$message = $extension->decodeMessage($message, $rsv);
		}
		if ($this->_maxMessageSize > 0 && strlen($message) > $this->_maxMessageSize) {
			throw (new TWebSocketException('websocket_message_too_big', $this->_maxMessageSize))
				->setCloseCode(TWebSocketCloseCode::MessageTooBig);
		}
		if ($isText && !self::isValidUtf8($message)) {
			throw (new TWebSocketException('websocket_text_not_utf8'))
				->setCloseCode(TWebSocketCloseCode::InvalidFramePayload);
		}
		return $message;
	}

	/**
	 * Validates a received Close frame's payload: a one-byte payload is malformed, the close code
	 * must be valid to receive, and any reason phrase must be valid UTF-8.  An empty payload (no
	 * code) is valid.
	 * @param TWebSocketFrame $frame The Close frame.
	 * @throws TWebSocketException When the payload, close code, or reason is invalid.
	 */
	private function validateCloseFrame(TWebSocketFrame $frame): void
	{
		$length = strlen($frame->getPayload());
		if ($length === 0) {
			return;
		}
		if ($length === 1) {
			throw new TWebSocketException('websocket_close_frame_invalid');
		}
		$code = $frame->getCloseCode();
		if ($code === null || !TWebSocketCloseCode::isValidIncoming($code)) {
			throw new TWebSocketException('websocket_close_code_invalid', (int) $code);
		}
		if (!self::isValidUtf8($frame->getCloseReason())) {
			throw (new TWebSocketException('websocket_close_reason_not_utf8'))
				->setCloseCode(TWebSocketCloseCode::InvalidFramePayload);
		}
	}

	/**
	 * Clears the message reassembly state after a message completes or fails.
	 */
	private function resetMessage(): void
	{
		$this->_messageBuffer = '';
		$this->_fragmentOpcode = null;
		$this->_fragmentRsv = 0;
	}

	/**
	 * Indicates whether a string is valid UTF-8.
	 * @param string $value The bytes to test.
	 * @return bool Whether the bytes are valid UTF-8.
	 */
	private static function isValidUtf8(string $value): bool
	{
		return $value === '' || preg_match('//u', $value) === 1;
	}

	/** @return ?string The negotiated subprotocol, or null when none was selected. */
	public function getSubprotocol(): ?string
	{
		return $this->_subprotocol;
	}

	/**
	 * Sets the negotiated subprotocol.
	 * @param ?string $value The subprotocol, or null for none.
	 */
	public function setSubprotocol(?string $value): void
	{
		$this->_subprotocol = $value;
	}

	/** @return IWebSocketExtension[] The negotiated extensions, in pipeline order. */
	public function getExtensions(): array
	{
		return $this->_extensions;
	}

	/**
	 * Sets the negotiated extensions applied per message, in pipeline order (encoded in order,
	 * decoded in reverse).  Two extensions must not reserve the same RSV bit.
	 * @param IWebSocketExtension[] $value The extensions.
	 * @throws TWebSocketException When an entry is not an extension or two reserve the same RSV bit.
	 */
	public function setExtensions(array $value): void
	{
		$reserved = 0;
		foreach ($value as $extension) {
			if (!$extension instanceof IWebSocketExtension) {
				throw new TWebSocketException('websocket_extension_invalid');
			}
			if (($reserved & $extension->getReservedRsv()) !== 0) {
				throw new TWebSocketException('websocket_extension_rsv_conflict', $extension->getName());
			}
			$reserved |= $extension->getReservedRsv();
		}
		$this->_extensions = array_values($value);
		$this->applyDecodeLimitToExtensions();
	}

	/**
	 * Propagates the message-size limit to any extension that bounds its decoded output (currently
	 * {@see TPermessageDeflateExtension}), so a decompression bomb is stopped mid-inflate rather than
	 * only after the fully expanded message has been allocated.
	 */
	private function applyDecodeLimitToExtensions(): void
	{
		foreach ($this->_extensions as $extension) {
			if ($extension instanceof TPermessageDeflateExtension) {
				$extension->setMaxOutputLength($this->_maxMessageSize);
			}
		}
	}

	/** @return bool Whether the RFC 6455 mask rule is enforced on received frames. */
	public function getValidateMasking(): bool
	{
		return $this->_validateMasking;
	}

	/**
	 * Sets whether to enforce the RFC 6455 mask rule on received frames.  A standalone server keeps
	 * this enabled; the HTTP/2 transport disables it, since RFC 8441 carries its own framing.
	 * @param bool $value Whether to enforce the mask rule.
	 */
	public function setValidateMasking(bool $value): void
	{
		$this->_validateMasking = $value;
	}

	/** @return int The maximum reassembled message size in bytes, or 0 for unlimited. */
	public function getMaxMessageSize(): int
	{
		return $this->_maxMessageSize;
	}

	/**
	 * Sets the maximum reassembled message size.  It bounds the read buffer (an oversized frame is
	 * rejected from its header before its payload is buffered), the reassembled message, and any
	 * extension's decoded output; a message that exceeds it fails the connection with
	 * {@see TWebSocketCloseCode::MessageTooBig}.
	 * @param int $value The maximum size in bytes, or 0 for unlimited.
	 */
	public function setMaxMessageSize(int $value): void
	{
		$this->_maxMessageSize = max(0, $value);
		$this->applyDecodeLimitToExtensions();
	}

	/** @return int The maximum queued outbound bytes before the connection is dropped, or 0 for unlimited. */
	public function getMaxSendBufferBytes(): int
	{
		return $this->_maxSendBufferBytes;
	}

	/**
	 * Sets the maximum queued outbound bytes tolerated for a slow reader before the connection is
	 * dropped.  It bounds the memory one non-draining peer can hold, replacing an unbounded (or
	 * blocking) wait; 0 is unlimited.
	 * @param int $value The maximum queued outbound bytes.
	 */
	public function setMaxSendBufferBytes(int $value): void
	{
		$this->_maxSendBufferBytes = max(0, $value);
	}

	/**
	 * Raised when a Ping is received (a Pong is sent automatically afterward).
	 * @param mixed $param The Ping payload.
	 */
	public function onPing(mixed $param): void
	{
		$this->raiseEvent('onPing', $this, $param);
	}

	/**
	 * Raised when a Pong is received.
	 * @param mixed $param The Pong payload.
	 */
	public function onPong(mixed $param): void
	{
		$this->raiseEvent('onPong', $this, $param);
	}

	/**
	 * Raised when the peer sends a Close frame.
	 * @param mixed $param The Close {@see TWebSocketFrame}.
	 */
	public function onClose(mixed $param): void
	{
		$this->raiseEvent('onClose', $this, $param);
	}
}
