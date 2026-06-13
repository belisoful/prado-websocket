<?php

/**
 * TWebSocketFrame class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\TComponent;

/**
 * TWebSocketFrame class.
 *
 * A logical RFC 6455 frame: the FIN flag, the three RSV bits, an opcode
 * ({@see TWebSocketOpcode}), and the payload.  Masking is a wire concern applied by
 * {@see TWebSocketFrameCodec} on encode, so it is not part of the frame.
 *
 * The static factories build the common frames: {@see text()}, {@see binary()},
 * {@see continuation()}, {@see ping()}, {@see pong()}, and {@see close()}.  For a Close frame,
 * {@see getCloseCode()} and {@see getCloseReason()} read the two-byte code and the reason.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 4.4.0
 * @see https://www.rfc-editor.org/rfc/rfc6455.html#section-5.2
 */
class TWebSocketFrame extends TComponent
{
	/** @var bool Whether this is the final frame of a message. */
	private bool $_fin;

	/** @var int The opcode (a {@see TWebSocketOpcode} constant). */
	private int $_opcode;

	/** @var string The frame payload (unmasked). */
	private string $_payload;

	/** @var bool The RSV1 reserved bit (extension-defined). */
	private bool $_rsv1;

	/** @var bool The RSV2 reserved bit (extension-defined). */
	private bool $_rsv2;

	/** @var bool The RSV3 reserved bit (extension-defined). */
	private bool $_rsv3;

	/**
	 * @param int $opcode The opcode (a {@see TWebSocketOpcode} constant).
	 * @param string $payload The unmasked payload.
	 * @param bool $fin Whether this is the final frame of a message. Default true.
	 * @param bool $rsv1 The RSV1 bit. Default false.
	 * @param bool $rsv2 The RSV2 bit. Default false.
	 * @param bool $rsv3 The RSV3 bit. Default false.
	 */
	public function __construct(int $opcode, string $payload = '', bool $fin = true, bool $rsv1 = false, bool $rsv2 = false, bool $rsv3 = false)
	{
		$this->_opcode = $opcode;
		$this->_payload = $payload;
		$this->_fin = $fin;
		$this->_rsv1 = $rsv1;
		$this->_rsv2 = $rsv2;
		$this->_rsv3 = $rsv3;
		parent::__construct();
	}

	/** Builds a Text data frame. @param string $data The UTF-8 text. @param bool $fin Whether final. @return self The frame. */
	public static function text(string $data, bool $fin = true): self
	{
		return new self(TWebSocketOpcode::Text, $data, $fin);
	}

	/** Builds a Binary data frame. @param string $data The bytes. @param bool $fin Whether final. @return self The frame. */
	public static function binary(string $data, bool $fin = true): self
	{
		return new self(TWebSocketOpcode::Binary, $data, $fin);
	}

	/** Builds a Continuation frame. @param string $data The bytes. @param bool $fin Whether final. @return self The frame. */
	public static function continuation(string $data, bool $fin = true): self
	{
		return new self(TWebSocketOpcode::Continuation, $data, $fin);
	}

	/** Builds a Ping control frame. @param string $data The application data (<=125 bytes). @return self The frame. */
	public static function ping(string $data = ''): self
	{
		return new self(TWebSocketOpcode::Ping, $data);
	}

	/** Builds a Pong control frame. @param string $data The application data (<=125 bytes). @return self The frame. */
	public static function pong(string $data = ''): self
	{
		return new self(TWebSocketOpcode::Pong, $data);
	}

	/**
	 * Builds a Close control frame.
	 * @param ?int $code A {@see TWebSocketCloseCode} value, or null for an empty Close.
	 * @param string $reason A UTF-8 reason phrase. Default ''.
	 * @return self The frame.
	 */
	public static function close(?int $code = null, string $reason = ''): self
	{
		$payload = $code === null ? '' : pack('n', $code) . $reason;
		return new self(TWebSocketOpcode::Close, $payload);
	}

	/** @return bool Whether this is the final frame of a message. */
	public function getFin(): bool
	{
		return $this->_fin;
	}

	/** @return int The opcode (a {@see TWebSocketOpcode} constant). */
	public function getOpcode(): int
	{
		return $this->_opcode;
	}

	/** @return string The unmasked payload. */
	public function getPayload(): string
	{
		return $this->_payload;
	}

	/** @return bool The RSV1 bit. */
	public function getRsv1(): bool
	{
		return $this->_rsv1;
	}

	/** @return bool The RSV2 bit. */
	public function getRsv2(): bool
	{
		return $this->_rsv2;
	}

	/** @return bool The RSV3 bit. */
	public function getRsv3(): bool
	{
		return $this->_rsv3;
	}

	/** @return bool Whether this is a control frame (Close, Ping, Pong). */
	public function getIsControl(): bool
	{
		return TWebSocketOpcode::isControl($this->_opcode);
	}

	/**
	 * Returns the close code from a Close frame's payload.
	 * @return ?int The close code, or null when this is not a Close frame with a code.
	 */
	public function getCloseCode(): ?int
	{
		if ($this->_opcode !== TWebSocketOpcode::Close || strlen($this->_payload) < 2) {
			return null;
		}
		return unpack('n', substr($this->_payload, 0, 2))[1];
	}

	/**
	 * Returns the reason phrase from a Close frame's payload.
	 * @return string The reason phrase, or '' when absent.
	 */
	public function getCloseReason(): string
	{
		if ($this->_opcode !== TWebSocketOpcode::Close || strlen($this->_payload) < 2) {
			return '';
		}
		return substr($this->_payload, 2);
	}
}
