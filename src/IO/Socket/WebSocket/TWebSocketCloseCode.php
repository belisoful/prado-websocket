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
 * Enumerates the RFC 6455 close codes carried in a Close control frame's two-byte payload.
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
 * | TLSHandshake        | 1015 | Reserved: TLS handshake failure.                     |
 *
 * The reserved codes (1005, 1006, 1015) are status values only; they must not be sent on the
 * wire, which {@see isSendable()} reports.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc6455.html#section-7.4
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
	public const TLSHandshake = 1015;

	/**
	 * Indicates whether a close code may be sent on the wire.  The reserved codes 1005, 1006,
	 * and 1015 are status-only; the application range 3000-4999 and the protocol range
	 * 1000-1003 / 1007-1011 are sendable.
	 * @param int $code The close code.
	 * @return bool Whether the code may be sent in a Close frame.
	 */
	public static function isSendable(int $code): bool
	{
		if ($code === self::NoStatusReceived || $code === self::Abnormal || $code === self::TLSHandshake || $code === 1004) {
			return false;   // 1004 is reserved with no meaning, alongside the status-only 1005/1006/1015
		}
		return ($code >= 1000 && $code <= 1011) || ($code >= 3000 && $code <= 4999);
	}

	/**
	 * Indicates whether a close code is valid in a received Close frame.  The status-only codes
	 * (1004, 1005, 1006, 1015), codes below 1000, and the unassigned 1012-2999 range are rejected;
	 * the protocol codes 1000-1003 / 1007-1011 and the application range 3000-4999 are accepted.
	 * @param int $code The close code from a received Close frame.
	 * @return bool Whether the code is valid to receive.
	 */
	public static function isValidIncoming(int $code): bool
	{
		return ($code >= 1000 && $code <= 1003) || ($code >= 1007 && $code <= 1011) || ($code >= 3000 && $code <= 4999);
	}
}
