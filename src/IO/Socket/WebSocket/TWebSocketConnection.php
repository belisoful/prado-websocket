<?php

/**
 * TWebSocketConnection class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

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
 * @since 4.4.0
 * @see https://www.rfc-editor.org/rfc/rfc6455.html
 */
class TWebSocketConnection extends TComponent
{
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
	 * Accepts a server connection: runs the server handshake on the stream, then wraps it.
	 * @param StreamInterface $stream The accepted transport stream.
	 * @param array<string, string> $extraHeaders Extra response headers.
	 * @return self The server-side connection.
	 */
	public static function accept(StreamInterface $stream, array $extraHeaders = []): self
	{
		TWebSocketHandshake::acceptConnection($stream, $extraHeaders);
		return Prado::createComponent(self::class, $stream, false);
	}

	/**
	 * Opens a client connection: runs the client handshake on the stream, then wraps it.
	 * @param StreamInterface $stream The connected transport stream.
	 * @param string $host The Host header value.
	 * @param string $path The request target. Default '/'.
	 * @param array<string, string> $extraHeaders Extra request headers.
	 * @return self The client-side connection.
	 */
	public static function connect(StreamInterface $stream, string $host, string $path = '/', array $extraHeaders = []): self
	{
		TWebSocketHandshake::openConnection($stream, $host, $path, $extraHeaders);
		return Prado::createComponent(self::class, $stream, true);
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

	/** @return ?int The opcode of the most recently received data message, or null. */
	public function getLastOpcode(): ?int
	{
		return $this->_lastOpcode;
	}

	/**
	 * Encodes and writes a frame, masking it on the client side.
	 * @param TWebSocketFrame $frame The frame to send.
	 * @return int The number of bytes written.
	 */
	public function sendFrame(TWebSocketFrame $frame): int
	{
		$maskKey = $this->_isClient ? random_bytes(4) : null;
		return $this->_stream->write(TWebSocketFrameCodec::encode($frame, $maskKey));
	}

	/** Sends a Text message. @param string $text The UTF-8 text. @return int Bytes written. */
	public function send(string $text): int
	{
		return $this->sendFrame(TWebSocketFrame::text($text));
	}

	/** Sends a Binary message. @param string $data The bytes. @return int Bytes written. */
	public function sendBinary(string $data): int
	{
		return $this->sendFrame(TWebSocketFrame::binary($data));
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
	 * Reads and returns the next single frame, handling control frames inline (auto-Pong,
	 * close handshake).  Useful for an event loop that pumps one frame at a time; the
	 * message-level {@see receive()} builds on it.
	 * @return ?TWebSocketFrame The frame, or null at a clean end of stream.
	 */
	public function receiveFrame(): ?TWebSocketFrame
	{
		$frame = TWebSocketFrameCodec::decode($this->_stream);
		if ($frame === null) {
			$this->_closed = true;
			return null;
		}
		if ($frame->getIsControl()) {
			$this->handleControlFrame($frame);
		} elseif ($frame->getOpcode() !== TWebSocketOpcode::Continuation) {
			$this->_lastOpcode = $frame->getOpcode();
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
		$message = '';
		while (!$this->_closed) {
			$frame = $this->receiveFrame();
			if ($frame === null || $frame->getOpcode() === TWebSocketOpcode::Close) {
				return null;
			}
			if ($frame->getIsControl()) {
				continue;
			}
			$message .= $frame->getPayload();
			if ($frame->getFin()) {
				return $message;
			}
		}
		return null;
	}

	/**
	 * Feeds received bytes and returns the complete data messages they yield (non-blocking).
	 *
	 * This is the event-loop counterpart to {@see receive()}: a {@see select()}-driven server
	 * reads whatever bytes are available and feeds them here without blocking.  Bytes are
	 * buffered across calls, every complete frame is extracted, control frames are handled inline
	 * (auto-Pong, close handshake), and fragmented messages are reassembled.  A Close frame marks
	 * the connection {@see getIsClosed() closed}.
	 *
	 * @param string $bytes The bytes just read from the transport.
	 * @throws TWebSocketException When a frame is malformed (the caller should close).
	 * @return string[] The complete data messages, in order (empty when none completed).
	 */
	public function feed(string $bytes): array
	{
		$this->_readBuffer .= $bytes;
		$messages = [];
		while (!$this->_closed && ($decoded = TWebSocketFrameCodec::tryDecode($this->_readBuffer)) !== null) {
			$this->_readBuffer = substr($this->_readBuffer, $decoded['length']);
			$frame = $decoded['frame'];
			if ($frame->getIsControl()) {
				$this->handleControlFrame($frame);
				continue;
			}
			if ($frame->getOpcode() !== TWebSocketOpcode::Continuation) {
				$this->_lastOpcode = $frame->getOpcode();
			}
			$this->_messageBuffer .= $frame->getPayload();
			if ($frame->getFin()) {
				$messages[] = $this->_messageBuffer;
				$this->_messageBuffer = '';
			}
		}
		return $messages;
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
