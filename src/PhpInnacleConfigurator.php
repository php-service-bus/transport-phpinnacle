<?php

/**
 * PHPinnacle RabbitMQ adapter.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Transport\PhpInnacle;

use function Amp\call;
use Amp\Promise;
use PHPinnacle\Ridge\Channel;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ServiceBus\Transport\Amqp\AmqpExchange;
use ServiceBus\Transport\Amqp\AmqpQueue;
use ServiceBus\Transport\Common\Exceptions\BindFailed;
use ServiceBus\Transport\Common\Exceptions\CreateQueueFailed;
use ServiceBus\Transport\Common\Exceptions\CreateTopicFailed;

/**
 * Creating exchangers\queues and bind them.
 *
 * @internal
 */
final class PhpInnacleConfigurator
{
    /**
     * @var Channel
     */
    private $channel;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @param Channel              $channel
     * @param LoggerInterface|null $logger
     */
    public function __construct(Channel $channel, ?LoggerInterface $logger = null)
    {
        $this->channel = $channel;
        $this->logger  = $logger ?? new NullLogger();
    }

    /**
     * Execute queue creation.
     *
     * @param AmqpQueue $queue
     *
     * @throws \ServiceBus\Transport\Common\Exceptions\CreateQueueFailed
     *
     * @return Promise
     */
    public function doCreateQueue(AmqpQueue $queue): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
            function(AmqpQueue $queue): \Generator
            {
                try
                {
                    $this->logger->info('Creating "{queueName}" queue', ['queueName' => (string) $queue]);

                    /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                    yield $this->channel->queueDeclare(
                        (string) $queue,
                        $queue->isPassive(),
                        $queue->isDurable(),
                        $queue->isExclusive(),
                        $queue->autoDeleteEnabled(),
                        false,
                        $queue->arguments()
                    );
                }
                catch (\Throwable $throwable)
                {
                    throw CreateQueueFailed::fromThrowable($throwable);
                }
            },
            $queue
        );
    }

    /**
     * Bind queue to exchange(s).
     *
     * @psalm-param array<mixed, \ServiceBus\Transport\Common\QueueBind> $binds
     *
     * @param AmqpQueue   $queue
     * @param \ServiceBus\Transport\Common\QueueBind[] $binds
     *
     * @throws \ServiceBus\Transport\Common\Exceptions\BindFailed
     *
     * @return Promise
     */
    public function doBindQueue(AmqpQueue $queue, array $binds): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
            function(AmqpQueue $queue, array $binds): \Generator
            {
                try
                {
                    /** @var \ServiceBus\Transport\Common\QueueBind $bind */
                    foreach ($binds as $bind)
                    {
                        /** @var AmqpExchange $destinationExchange */
                        $destinationExchange = $bind->destinationTopic;

                        yield $this->doCreateExchange($destinationExchange);

                        $this->logger->info(
                            'Linking "{queueName}" queue to the exchange "{exchangeName}" with the routing key "{routingKey}"',
                            [
                                'queueName'    => (string) $queue,
                                'exchangeName' => (string) $destinationExchange,
                                'routingKey'   => (string) $bind->routingKey,
                            ]
                        );

                        /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                        yield $this->channel->queueBind((string) $queue, (string) $destinationExchange, (string) $bind->routingKey);
                    }
                }
                catch (\Throwable $throwable)
                {
                    throw BindFailed::fromThrowable($throwable);
                }
            },
            $queue,
            $binds
        );
    }

    /**
     * Execute exchange creation.
     *
     * @param AmqpExchange $exchange
     *
     * @throws \ServiceBus\Transport\Common\Exceptions\CreateTopicFailed
     *
     * @return Promise
     */
    public function doCreateExchange(AmqpExchange $exchange): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
            function(AmqpExchange $exchange): \Generator
            {
                try
                {
                    $this->logger->info('Creating "{exchangeName}" exchange', ['exchangeName' => (string) $exchange]);

                    /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                    yield $this->channel->exchangeDeclare(
                        (string) $exchange,
                        $exchange->type(),
                        $exchange->isPassive(),
                        $exchange->isDurable(),
                        false,
                        false,
                        false,
                        $exchange->arguments()
                    );
                }
                catch (\Throwable $throwable)
                {
                    throw CreateTopicFailed::fromThrowable($throwable);
                }
            },
            $exchange
        );
    }

    /**
     * Bind exchange to another exchange(s).
     *
     * @psalm-param array<mixed, \ServiceBus\Transport\Common\TopicBind> $binds
     *
     * @param AmqpExchange $exchange
     * @param \ServiceBus\Transport\Common\TopicBind[]  $binds
     *
     * @throws \ServiceBus\Transport\Common\Exceptions\BindFailed
     *
     * @return Promise
     */
    public function doBindExchange(AmqpExchange $exchange, array $binds): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
            function(AmqpExchange $exchange, array $binds): \Generator
            {
                try
                {
                    /** @var \ServiceBus\Transport\Common\TopicBind $bind */
                    foreach ($binds as $bind)
                    {
                        /** @var AmqpExchange $sourceExchange */
                        $sourceExchange = $bind->destinationTopic;

                        yield $this->doCreateExchange($sourceExchange);

                        $this->logger->info(
                            'Linking "{exchangeName}" exchange to the exchange "{destinationExchangeName}" with the routing key "{routingKey}"',
                            [
                                'queueName'               => (string) $sourceExchange,
                                'destinationExchangeName' => (string) $exchange,
                                'routingKey'              => (string) $bind->routingKey,
                            ]
                        );

                        /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                        yield $this->channel->exchangeBind((string) $sourceExchange, (string) $exchange, (string) $bind->routingKey);
                    }
                }
                catch (\Throwable $throwable)
                {
                    throw BindFailed::fromThrowable($throwable);
                }
            },
            $exchange,
            $binds
        );
    }
}
