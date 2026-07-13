<?php

/**
 * IWebSocketExtension interface file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

/**
 * IWebSocketExtension interface.
 *
 * A negotiated RFC 6455 extension that transforms message payloads on the wire.  Extensions are
 * agreed during the handshake through {@see \Prado\Web\THttpHeaderName::SecWebSocketExtensions} and
 * applied by a {@see TWebSocketConnection} as an ordered pipeline: {@see encodeMessage()} folds over
 * the list in order on send, and {@see decodeMessage()} folds in reverse on receive.
 *
 * An extension carries its transform state across the message it produces (compression context, a
 * cipher state) and signals its work through the frame's reserved bits.  RFC 6455 provides three:
 * {@see RSV1}, {@see RSV2}, {@see RSV3}.  {@see getReservedRsv()} declares which bits the extension
 * owns, so the connection allows them on a data message's first frame and rejects any reserved bit
 * no negotiated extension owns.  Two extensions on one connection must not reserve the same bit.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc6455.html#section-9
 */
interface IWebSocketExtension
{
	/** @var int The RSV1 reserved bit. */
	public const RSV1 = 0x4;

	/** @var int The RSV2 reserved bit. */
	public const RSV2 = 0x2;

	/** @var int The RSV3 reserved bit. */
	public const RSV3 = 0x1;

	/**
	 * Returns the extension token as it appears in the negotiation header.
	 * @return string The extension name (e.g. 'permessage-deflate').
	 */
	public function getName(): string;

	/**
	 * Returns the reserved bits this extension owns, a mask of {@see RSV1}, {@see RSV2}, {@see RSV3}.
	 * @return int The reserved-bit mask.
	 */
	public function getReservedRsv(): int;

	/**
	 * Transforms an outgoing message payload for the wire, reporting the reserved bits it sets.
	 * @param string $payload The message payload at this stage of the pipeline.
	 * @return array{0: string, 1: int} The transformed payload and the reserved bits it set.
	 */
	public function encodeMessage(string $payload): array;

	/**
	 * Reverses the transform on a received message payload, given the message's first-frame reserved
	 * bits.  An extension whose bit is unset leaves the payload unchanged.
	 * @param string $payload The message payload at this stage of the pipeline.
	 * @param int $rsv The reserved bits set on the message's first frame.
	 * @return string The reversed payload.
	 */
	public function decodeMessage(string $payload, int $rsv): string;
}
