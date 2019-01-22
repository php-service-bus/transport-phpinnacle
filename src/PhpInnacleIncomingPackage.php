<?php

/**
 * phpinnacle transport module for PHP Service Bus
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Transport\Amqp\PhpInnacle;

use function Amp\call;
use Amp\Promise;
use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Message;
use function ServiceBus\Common\uuid;
use ServiceBus\Transport\Amqp\AmqpTransportLevelDestination;
use ServiceBus\Transport\Common\DeliveryDestination;
use ServiceBus\Transport\Common\Exceptions\AcknowledgeFailed;
use ServiceBus\Transport\Common\Exceptions\NotAcknowledgeFailed;
use ServiceBus\Transport\Common\Exceptions\RejectFailed;
use ServiceBus\Transport\Common\Package\IncomingPackage;
use ServiceBus\Transport\Common\Transport;

/**
 *
 */
final class PhpInnacleIncomingPackage implements IncomingPackage
{
    /**
     * Received package id
     *
     * @var string
     */
    private $id;

    /**
     * The time the message was received (Unix timestamp with microseconds)
     *
     * @var float
     */
    private $time;

    /**
     * @var Message
     */
    private $originMessage;

    /**
     * @var Channel
     */
    private $channel;

    /**
     * @param Message $message
     * @param Channel $channel
     *
     * @return self
     */
    public static function received(Message $message, Channel $channel): self
    {
        $self = new self();

        $self->channel       = $channel;
        $self->originMessage = $message;

        return $self;
    }

    /**
     * @inheritDoc
     */
    public function id(): string
    {
        return $this->id;
    }

    /**
     * @inheritDoc
     */
    public function time(): float
    {
        return $this->time;
    }

    /**
     * @inheritDoc
     */
    public function origin(): DeliveryDestination
    {
        return AmqpTransportLevelDestination::create(
            $this->originMessage->exchange(),
            $this->originMessage->routingKey()
        );
    }

    /**
     * @inheritDoc
     */
    public function payload(): string
    {
        return $this->originMessage->content();
    }

    /**
     * @inheritDoc
     */
    public function headers(): array
    {
        return $this->originMessage->headers();
    }

    /**
     * @inheritDoc
     */
    public function ack(): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(): \Generator
            {
                try
                {
                    yield $this->channel->ack($this->originMessage);
                }
                catch(\Throwable $throwable)
                {
                    throw new AcknowledgeFailed($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
                }
            }
        );
    }

    /**
     * @inheritDoc
     */
    public function nack(bool $requeue, ?string $withReason = null): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(bool $requeue): \Generator
            {
                try
                {
                    yield $this->channel->nack($this->originMessage, false, $requeue);
                }
                catch(\Throwable $throwable)
                {
                    throw new NotAcknowledgeFailed($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
                }
            },
            $requeue
        );
    }

    /**
     * @inheritDoc
     */
    public function reject(bool $requeue, ?string $withReason = null): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(bool $requeue): \Generator
            {
                try
                {
                    yield $this->channel->reject($this->originMessage, $requeue);
                }
                catch(\Throwable $throwable)
                {
                    throw new RejectFailed($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
                }
            },
            $requeue
        );
    }

    /**
     * @inheritDoc
     */
    public function traceId(): string
    {
        $traceId = (string) $this->originMessage->header(Transport::SERVICE_BUS_TRACE_HEADER);

        if('' === $traceId)
        {
            $traceId = uuid();
        }

        return $traceId;
    }

    /**
     *
     */
    private function __construct()
    {
        $this->id   = uuid();
        $this->time = (float) \microtime(true);
    }
}
