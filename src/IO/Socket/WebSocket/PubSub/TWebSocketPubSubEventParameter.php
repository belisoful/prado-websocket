<?php

/**
 * TWebSocketPubSubEventParameter class file.
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\PubSub;

use Prado\TEventParameter;
use Prado\TPropertyValue;

/**
 * TWebSocketPubSubEventParameter class.
 *
 * The parameter of the {@see TWebSocketPubSubHandler} request events.  The event sender is the
 * requesting {@see \Prado\IO\Socket\WebSocket\TWebSocketConnection}.  The event
 * {@see getParameter() Parameter} is the request payload, also reachable as {@see getData() Data}.
 *
 * | Event | Fields set | Handler sets |
 * |---|---|---|
 * | `onSubscribe` | ClientId, Channel | Allowed |
 * | `onPublish` | ClientId, Channel, Data | Allowed, Data |
 * | `onSend` | ClientId, To, Data | Allowed, Data |
 * | `onCall` | ClientId, Method, Data (the params) | Result |
 *
 * Setting {@see setResult() Result} marks the call {@see getHandled() Handled}; a call no handler
 * answers is replied to with `not_found`.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.2.0
 */
class TWebSocketPubSubEventParameter extends TEventParameter
{
	/** @var string The requesting client's cluster id. */
	private string $_clientId;

	/** @var ?string The channel of a subscribe or publish request. */
	private ?string $_channel = null;

	/** @var ?string The target client id of a send request. */
	private ?string $_to = null;

	/** @var ?string The method name of a call request. */
	private ?string $_method = null;

	/** @var bool Whether the request may proceed. */
	private bool $_allowed;

	/** @var mixed The call result. */
	private mixed $_result = null;

	/** @var bool Whether a handler answered the call. */
	private bool $_handled = false;

	/**
	 * @param string $clientId The requesting client's cluster id.
	 * @param mixed $data The request payload.
	 * @param bool $allowed Whether the request proceeds when no handler changes it.
	 */
	public function __construct(string $clientId, mixed $data = null, bool $allowed = true)
	{
		$this->_clientId = $clientId;
		$this->_allowed = $allowed;
		parent::__construct($data);
	}

	/**
	 * Returns the requesting client's cluster id.
	 * @return string The client id.
	 */
	public function getClientId(): string
	{
		return $this->_clientId;
	}

	/**
	 * Returns the channel of a subscribe or publish request.
	 * @return ?string The channel, or null for other requests.
	 */
	public function getChannel(): ?string
	{
		return $this->_channel;
	}

	/**
	 * Sets the channel of a subscribe or publish request.
	 * @param ?string $value The channel.
	 * @return static The current parameter.
	 */
	public function setChannel(?string $value): static
	{
		$this->_channel = $value;
		return $this;
	}

	/**
	 * Returns the target client id of a send request.
	 * @return ?string The target client id, or null for other requests.
	 */
	public function getTo(): ?string
	{
		return $this->_to;
	}

	/**
	 * Sets the target client id of a send request.
	 * @param ?string $value The target client id.
	 * @return static The current parameter.
	 */
	public function setTo(?string $value): static
	{
		$this->_to = $value;
		return $this;
	}

	/**
	 * Returns the method name of a call request.
	 * @return ?string The method name, or null for other requests.
	 */
	public function getMethod(): ?string
	{
		return $this->_method;
	}

	/**
	 * Sets the method name of a call request.
	 * @param ?string $value The method name.
	 * @return static The current parameter.
	 */
	public function setMethod(?string $value): static
	{
		$this->_method = $value;
		return $this;
	}

	/**
	 * Returns the request payload: the published or sent data, or the call params.
	 * @return mixed The payload.
	 */
	public function getData(): mixed
	{
		return $this->getParameter();
	}

	/**
	 * Sets the request payload, replacing what is published or sent.
	 * @param mixed $value The payload.
	 */
	public function setData(mixed $value): void
	{
		$this->setParameter($value);
	}

	/**
	 * Returns whether the request may proceed.
	 * @return bool Whether the request is allowed.
	 */
	public function getAllowed(): bool
	{
		return $this->_allowed;
	}

	/**
	 * Sets whether the request may proceed.
	 * @param bool|string $value Whether the request is allowed.
	 * @return static The current parameter.
	 */
	public function setAllowed($value): static
	{
		$this->_allowed = TPropertyValue::ensureBoolean($value);
		return $this;
	}

	/**
	 * Returns the call result sent back in the `ack` frame.
	 * @return mixed The result.
	 */
	public function getResult(): mixed
	{
		return $this->_result;
	}

	/**
	 * Sets the call result and marks the call {@see getHandled() Handled}.
	 * @param mixed $value The result; it must be JSON-encodable.
	 * @return static The current parameter.
	 */
	public function setResult(mixed $value): static
	{
		$this->_result = $value;
		$this->_handled = true;
		return $this;
	}

	/**
	 * Returns whether a handler answered the call.
	 * @return bool Whether the call is handled.
	 */
	public function getHandled(): bool
	{
		return $this->_handled;
	}

	/**
	 * Sets whether a handler answered the call.
	 * @param bool|string $value Whether the call is handled.
	 * @return static The current parameter.
	 */
	public function setHandled($value): static
	{
		$this->_handled = TPropertyValue::ensureBoolean($value);
		return $this;
	}
}
