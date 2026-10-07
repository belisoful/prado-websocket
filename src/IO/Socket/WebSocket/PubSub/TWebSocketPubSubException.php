<?php

/**
 * TWebSocketPubSubException class file.
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\PubSub;

use Prado\IO\Socket\WebSocket\TWebSocketException;

/**
 * TWebSocketPubSubException class.
 *
 * Rejects one `prado.pubsub.v1` request.  {@see TWebSocketPubSubHandler} answers it with an
 * `error` frame carrying the {@see getReplyCode() ReplyCode} and the exception message; the
 * connection stays open.  An application event handler throws it to refuse a request with its own
 * reply code:
 *
 * ```php
 * throw (new TWebSocketPubSubException('websocket_pubsub_forbidden', 'call chat.history'))
 *     ->setReplyCode(TWebSocketPubSubException::FORBIDDEN);
 * ```
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.2.0
 */
class TWebSocketPubSubException extends TWebSocketException
{
	/** The frame is malformed or a field is invalid. */
	public const BAD_REQUEST = 'bad_request';

	/** The frame type is not part of the protocol. */
	public const UNKNOWN_TYPE = 'unknown_type';

	/** An authorization event vetoed the request. */
	public const FORBIDDEN = 'forbidden';

	/** The request exceeds a per-client limit. */
	public const LIMIT = 'limit';

	/** The target client or the called method does not exist. */
	public const NOT_FOUND = 'not_found';

	/** The server failed while processing the request. */
	public const INTERNAL = 'internal';

	/** @var string The code sent to the client in the `error` frame. */
	private string $_replyCode = self::BAD_REQUEST;

	/**
	 * Returns the code sent to the client in the `error` frame.
	 * @return string The reply code. Default {@see BAD_REQUEST}.
	 */
	public function getReplyCode(): string
	{
		return $this->_replyCode;
	}

	/**
	 * Sets the code sent to the client in the `error` frame.
	 * @param string $value The reply code.
	 * @return static This instance, for chaining at the throw site.
	 */
	public function setReplyCode(string $value): static
	{
		$this->_replyCode = $value;
		return $this;
	}
}
