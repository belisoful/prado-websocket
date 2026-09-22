<?php

/**
 * TWebSocketCloseCode class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\TEnumerable;

/**
 * TWebSocketCloseCode class.
 *
 * Enumerates the close codes carried in a Close control frame's two-byte payload: the RFC 6455
 * codes and the three IANA-assigned codes 1012-1014.
 *
 * | Constant            | Code | Meaning                                              |
 * |---------------------|------|------------------------------------------------------|
 * | Normal              | 1000 | Normal closure.                                      |
 * | GoingAway           | 1001 | Endpoint going away (server shutdown, page leaving). |
 * | ProtocolError       | 1002 | Protocol error.                                      |
 * | UnsupportedData     | 1003 | Unacceptable data type.                              |
 * | NoStatusReceived    | 1005 | Reserved: no status code present.                    |
 * | Abnormal            | 1006 | Reserved: closed without a Close frame.              |
 * | InvalidFramePayload | 1007 | Data not consistent with the message type (bad UTF-8). |
 * | PolicyViolation     | 1008 | Generic policy violation.                            |
 * | MessageTooBig       | 1009 | Message too big to process.                          |
 * | MandatoryExtension  | 1010 | A required extension was not negotiated.             |
 * | InternalServerError | 1011 | Unexpected condition.                                |
 * | ServiceRestart      | 1012 | The server is restarting.                            |
 * | TryAgainLater       | 1013 | Temporary condition (overload); retry later.         |
 * | BadGateway          | 1014 | An upstream server answered the gateway invalidly.   |
 * | TLSHandshake        | 1015 | Reserved: TLS handshake failure.                     |
 *
 * The code ranges: 1000-1003 and 1007-1014 are sendable and valid to receive; 1004 is reserved
 * without a meaning; 1005, 1006, and 1015 are status values only, never sent on the wire;
 * 1016-2999 are unassigned; 3000-3999 are registered by libraries and frameworks; 4000-4999 are
 * private application codes.  {@see isSendable()} and {@see isValidIncoming()} report the ranges.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc6455.html#section-7.4
 * @see https://www.iana.org/assignments/websocket/websocket.xhtml#close-code-number
 */
class TWebSocketCloseCode extends TEnumerable
{
	public const Normal = 1000;
	public const GoingAway = 1001;
	public const ProtocolError = 1002;
	public const UnsupportedData = 1003;
	public const NoStatusReceived = 1005;
	public const Abnormal = 1006;
	public const InvalidFramePayload = 1007;
	public const PolicyViolation = 1008;
	public const MessageTooBig = 1009;
	public const MandatoryExtension = 1010;
	public const InternalServerError = 1011;
	public const ServiceRestart = 1012;
	public const TryAgainLater = 1013;
	public const BadGateway = 1014;
	public const TLSHandshake = 1015;

	/**
	 * Indicates whether a close code may be sent on the wire: the protocol codes 1000-1003 and
	 * 1007-1014 and the application range 3000-4999.  The reserved 1004 and the status-only 1005,
	 * 1006, and 1015 are refused, as is the unassigned 1016-2999 range.
	 * @param int $code The close code.
	 * @return bool Whether the code may be sent in a Close frame.
	 */
	public static function isSendable(int $code): bool
	{
		return ($code >= 1000 && $code <= 1003) || ($code >= 1007 && $code <= 1014) || ($code >= 3000 && $code <= 4999);
	}

	/**
	 * Indicates whether a close code is valid in a received Close frame.  The valid set is the
	 * sendable set: a peer may only send what {@see isSendable()} permits.
	 * @param int $code The close code from a received Close frame.
	 * @return bool Whether the code is valid to receive.
	 */
	public static function isValidIncoming(int $code): bool
	{
		return self::isSendable($code);
	}
}
