<?php

/**
 * phpinnacle RabbitMQ adapter
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Transport\PhpInnacle;

use PHPinnacle\Ridge\Channel;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ServiceBus\Transport\Amqp\AmqpExchange;
use ServiceBus\Transport\Amqp\AmqpQueue;
use ServiceBus\Transport\Common\Exceptions\BindFailed;
use ServiceBus\Transport\Common\Exceptions\CreateQueueFailed;
use ServiceBus\Transport\Common\Exceptions\CreateTopicFailed;

/**
 * Creating exchangers\queues and bind them
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
     * Execute queue creation
     *
     * @param AmqpQueue $queue
     *
     * @return \Generator
     *
     * @throws \ServiceBus\Transport\Common\Exceptions\CreateQueueFailed
     */
    public function doCreateQueue(AmqpQueue $queue): \Generator
    {
        try
        {
            /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
            yield $this->channel->queueDeclare(
                (string) $queue, $queue->isPassive(), $queue->isDurable(), $queue->isExclusive(),
                $queue->autoDeleteEnabled(), false, $queue->arguments()
            );

            $this->logger->info('Created "{queueName}" queue', [
                'queueName' => (string) $queue
            ]);
        }
        catch(\Throwable $throwable)
        {
            throw new CreateQueueFailed($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
        }
    }

    /**
     * Bind queue to exchange(s)
     *
     * @param AmqpQueue                                            $queue
     * @param array<mixed, \ServiceBus\Transport\Common\QueueBind> $binds
     *
     * @return \Generator
     *
     * @throws \ServiceBus\Transport\Common\Exceptions\BindFailed
     */
    public function doBindQueue(AmqpQueue $queue, array $binds): \Generator
    {
        try
        {
            foreach($binds as $bind)
            {
                /** @var \ServiceBus\Transport\Common\QueueBind $bind */

                /** @var AmqpExchange $destinationExchange */
                $destinationExchange = $bind->destinationTopic;

                yield from $this->doCreateExchange($destinationExchange);

                /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                yield $this->channel->queueBind((string) $queue, (string) $destinationExchange, (string) $bind->routingKey);

                $this->logger->info(
                    'The queue "{queueName}" was tied to the exchange "{exchangeName}" with the routing key "{routingKey}"', [
                        'queueName'    => (string) $queue,
                        'exchangeName' => (string) $destinationExchange,
                        'routingKey'   => (string) $bind->routingKey
                    ]
                );
            }
        }
        catch(\Throwable $throwable)
        {
            throw new BindFailed($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
        }
    }

    /**
     * Execute exchange creation
     *
     * @param AmqpExchange $exchange
     *
     * @return \Generator
     *
     * @throws \ServiceBus\Transport\Common\Exceptions\CreateTopicFailed
     */
    public function doCreateExchange(AmqpExchange $exchange): \Generator
    {
        try
        {
            /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
            yield $this->channel->exchangeDeclare(
                (string) $exchange, $exchange->type(), $exchange->isPassive(), $exchange->isDurable(),
                false, false, false, $exchange->arguments()
            );

            $this->logger->info('Created "{exchangeName}" exchange', [
                'exchangeName' => (string) $exchange
            ]);
        }
        catch(\Throwable $throwable)
        {
            throw new CreateTopicFailed($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
        }
    }

    /**
     * Bind exchange to another exchange(s)
     *
     * @param AmqpExchange                                         $exchange
     * @param array<mixed, \ServiceBus\Transport\Common\TopicBind> $binds
     *
     * @return \Generator
     *
     * @throws \ServiceBus\Transport\Common\Exceptions\BindFailed
     */
    public function doBindExchange(AmqpExchange $exchange, array $binds): \Generator
    {
        try
        {
            foreach($binds as $bind)
            {
                /** @var \ServiceBus\Transport\Common\TopicBind $bind */

                /** @var AmqpExchange $sourceExchange */
                $sourceExchange = $bind->destinationTopic;

                yield from $this->doCreateExchange($sourceExchange);

                /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                yield $this->channel->exchangeBind((string) $sourceExchange, (string) $exchange, (string) $bind->routingKey);

                $this->logger->info(
                    'The exchange "{exchangeName}" was tied to the exchange "{destinationExchangeName}" with the routing key "{routingKey}"', [
                        'queueName'               => (string) $sourceExchange,
                        'destinationExchangeName' => (string) $exchange,
                        'routingKey'              => (string) $bind->routingKey
                    ]
                );
            }
        }
        catch(\Throwable $throwable)
        {
            throw new BindFailed($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
        }
    }
}
