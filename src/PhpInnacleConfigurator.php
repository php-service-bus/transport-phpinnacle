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

        if (false === \extension_loaded('ext-buffer'))
        {
            $this->logger->info(
                'Install a "ext-buffer" extension to improve performance (https://github.com/phpinnacle/ext-buffer)'
            );
        }
    }

    /**
     * Execute queue creation.
     *
     * @noinspection PhpDocRedundantThrowsInspection
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
                    $this->logger->info('Creating "{queueName}" queue', ['queueName' => $queue->name]);

                    yield $this->channel->queueDeclare(
                        $queue->name,
                        $queue->passive,
                        $queue->durable,
                        $queue->exclusive,
                        $queue->autoDelete,
                        true,
                        $queue->arguments
                    );
                }
                catch (\Throwable $throwable)
                {
                    $this->logger->error($throwable->getMessage(), [
                        'throwablePoint' => \sprintf('%s:%d', $throwable->getFile(), $throwable->getLine()),
                    ]);

                    throw CreateQueueFailed::fromThrowable($throwable);
                }
            },
            $queue
        );
    }

    /**
     * Bind queue to exchange(s).
     *
     * @noinspection PhpDocRedundantThrowsInspection
     *
     * @psalm-param  array<mixed, \ServiceBus\Transport\Common\QueueBind> $binds
     *
     * @param AmqpQueue                                $queue
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
                                'queueName'    => $queue->name,
                                'exchangeName' => $destinationExchange->name,
                                'routingKey'   => (string) $bind->routingKey,
                            ]
                        );

                        yield $this->channel->queueBind($queue->name, $destinationExchange->name, (string) $bind->routingKey);
                    }
                }
                catch (\Throwable $throwable)
                {
                    $this->logger->error($throwable->getMessage(), [
                        'throwablePoint' => \sprintf('%s:%d', $throwable->getFile(), $throwable->getLine()),
                    ]);

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
     * @noinspection PhpDocRedundantThrowsInspection
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
                    $this->logger->info('Creating "{exchangeName}" exchange', ['exchangeName' => $exchange->name]);

                    yield $this->channel->exchangeDeclare(
                        $exchange->name,
                        $exchange->type,
                        $exchange->passive,
                        $exchange->durable,
                        false,
                        false,
                        true,
                        $exchange->arguments
                    );
                }
                catch (\Throwable $throwable)
                {
                    $this->logger->error($throwable->getMessage(), [
                        'throwablePoint' => \sprintf('%s:%d', $throwable->getFile(), $throwable->getLine()),
                    ]);

                    throw CreateTopicFailed::fromThrowable($throwable);
                }
            },
            $exchange
        );
    }

    /**
     * Bind exchange to another exchange(s).
     *
     * @noinspection PhpDocRedundantThrowsInspection
     *
     * @psalm-param  array<mixed, \ServiceBus\Transport\Common\TopicBind> $binds
     *
     * @param AmqpExchange                             $exchange
     * @param \ServiceBus\Transport\Common\TopicBind[] $binds
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
                                'queueName'               => $sourceExchange->name,
                                'destinationExchangeName' => $exchange->name,
                                'routingKey'              => (string) $bind->routingKey,
                            ]
                        );

                        yield $this->channel->exchangeBind($sourceExchange->name, $exchange->name, (string) $bind->routingKey);
                    }
                }
                catch (\Throwable $throwable)
                {
                    $this->logger->error($throwable->getMessage(), [
                        'throwablePoint' => \sprintf('%s:%d', $throwable->getFile(), $throwable->getLine()),
                    ]);

                    throw BindFailed::fromThrowable($throwable);
                }
            },
            $exchange,
            $binds
        );
    }
}
