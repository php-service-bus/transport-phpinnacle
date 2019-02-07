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

use function Amp\call;
use Amp\Emitter;
use Amp\Promise;
use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Client;
use PHPinnacle\Ridge\Config;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ServiceBus\Transport\Amqp\AmqpConnectionConfiguration;
use ServiceBus\Transport\Amqp\AmqpExchange;
use ServiceBus\Transport\Amqp\AmqpQoSConfiguration;
use ServiceBus\Transport\Amqp\AmqpQueue;
use ServiceBus\Transport\Common\Exceptions\ConnectionFail;
use ServiceBus\Transport\Common\Package\OutboundPackage;
use ServiceBus\Transport\Common\Queue;
use ServiceBus\Transport\Common\QueueBind;
use ServiceBus\Transport\Common\Topic;
use ServiceBus\Transport\Common\TopicBind;
use ServiceBus\Transport\Common\Transport;

/**
 *
 */
final class PhpInnacleTransport implements Transport
{
    /**
     * Client for work with AMQP protocol
     *
     * @var Client
     */
    private $client;

    /**
     * Channel client
     *
     * Null if not connected
     *
     * @var Channel|null
     */
    private $channel;

    /**
     * Publisher
     *
     * @var PhpInnaclePublisher|null
     */
    private $publisher;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var array<string, \ServiceBus\Transport\PhpInnacle\PhpInnacleConsumer>
     */
    private $consumers = [];

    /**
     * @param AmqpConnectionConfiguration $connectionConfig
     * @param AmqpQoSConfiguration|null   $qosConfig
     * @param LoggerInterface|null        $logger
     */
    public function __construct(
        AmqpConnectionConfiguration $connectionConfig,
        AmqpQoSConfiguration $qosConfig = null,
        ?LoggerInterface $logger = null
    )
    {
        $qosConfig = $qosConfig ?? new AmqpQoSConfiguration();

        $this->logger = $logger ?? new NullLogger();
        $this->client = new Client($this->adaptConfig($connectionConfig, $qosConfig));
    }

    /**
     * @inheritDoc
     */
    public function connect(): Promise
    {
        /**
         * @psalm-suppress MixedTypeCoercion
         * @psalm-suppress InvalidArgument
         */
        return call(
            function(): \Generator
            {
                if(true === $this->client->isConnected())
                {
                    return;
                }

                try
                {
                    /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                    yield $this->client->connect();

                    /**
                     * @psalm-suppress TooManyTemplateParams Wrong Promise template
                     * @var Channel $channel
                     */
                    $channel = yield $this->client->channel();

                    $this->channel = $channel;

                    $this->logger->info('Connected to broker');
                }
                catch(\Throwable $throwable)
                {
                    throw new ConnectionFail($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
                }
            }
        );
    }

    /**
     * @inheritDoc
     */
    public function disconnect(): Promise
    {
        /**
         * @psalm-suppress MixedTypeCoercion
         * @psalm-suppress InvalidArgument
         */
        return call(
            function(): \Generator
            {
                try
                {
                    if(true === $this->client->isConnected())
                    {
                        /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                        yield $this->client->disconnect();
                    }
                }
                catch(\Throwable $throwable)
                {
                    /** Not interested */
                }
            }
        );
    }

    /**
     * @psalm-suppress MixedTypeCoercion
     *
     * @inheritDoc
     */
    public function consume(Queue $queue): Promise
    {
        /** @var AmqpQueue $queue */

        /** @psalm-suppress InvalidArgument */
        return call(
            function(AmqpQueue $queue): \Generator
            {
                yield $this->connect();

                /** @var Channel $channel */
                $channel  = $this->channel;
                $emitter  = new Emitter();
                $consumer = new PhpInnacleConsumer($queue, $channel, $this->logger);

                $consumer->listen(
                    static function(PhpInnacleIncomingPackage $incomingPackage) use ($emitter): \Generator
                    {
                        yield $emitter->emit($incomingPackage);
                    }
                );

                $this->consumers[(string) $queue] = $consumer;

                return $emitter->iterate();
            },
            $queue
        );
    }

    /**
     * @inheritDoc
     */
    public function stop(Queue $queue): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
            function(Queue $queue): \Generator
            {
                $queueName = (string) $queue;

                if(true === isset($this->consumers[$queueName]))
                {
                    /** @var PhpInnacleConsumer $consumer */
                    $consumer = $this->consumers[$queueName];

                    yield $consumer->stop();

                    unset($this->consumers[$queueName]);
                }
            }
        );
    }

    /**
     * @inheritDoc
     */
    public function send(OutboundPackage $outboundPackage): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
            function(OutboundPackage $outboundPackage): \Generator
            {
                yield $this->connect();

                /** @var Channel $channel */
                $channel = $this->channel;

                if(null === $this->publisher)
                {
                    $this->publisher = new PhpInnaclePublisher($channel, $this->logger);
                }

                yield $this->publisher->process($outboundPackage);
            },
            $outboundPackage
        );
    }

    /**
     * @inheritDoc
     */
    public function createTopic(Topic $topic, TopicBind ...$binds): Promise
    {
        /** @var AmqpExchange $topic */

        /** @psalm-suppress InvalidArgument */
        return call(
            function(AmqpExchange $exchange, array $binds): \Generator
            {
                /** @var array<mixed, \ServiceBus\Transport\Common\TopicBind> $binds */

                yield $this->connect();

                /** @var Channel $channel */
                $channel = $this->channel;

                $configurator = new PhpInnacleConfigurator($channel);

                yield from $configurator->doCreateExchange($exchange);
                yield from $configurator->doBindExchange($exchange, $binds);
            },
            $topic, $binds
        );
    }

    /**
     * @inheritDoc
     */
    public function createQueue(Queue $queue, QueueBind ...$binds): Promise
    {
        /** @var AmqpQueue $queue */

        /** @psalm-suppress InvalidArgument */
        return call(
            function(AmqpQueue $queue, array $binds): \Generator
            {
                /** @var array<mixed, \ServiceBus\Transport\Common\QueueBind> $binds */

                yield $this->connect();

                /** @var Channel $channel */
                $channel = $this->channel;

                $configurator = new PhpInnacleConfigurator($channel);

                yield from $configurator->doCreateQueue($queue);
                yield from $configurator->doBindQueue($queue, $binds);
            },
            $queue, $binds
        );
    }

    /**
     * Create phpinnacle configuration
     *
     * @param AmqpConnectionConfiguration $connectionConfiguration
     * @param AmqpQoSConfiguration        $qoSConfiguration
     *
     * @return Config
     */
    private function adaptConfig(
        AmqpConnectionConfiguration $connectionConfiguration,
        AmqpQoSConfiguration $qoSConfiguration
    ): Config
    {
        $config = new Config(
            $connectionConfiguration->host(),
            $connectionConfiguration->port(),
            $connectionConfiguration->virtualHost(),
            $connectionConfiguration->user(),
            $connectionConfiguration->password()
        );

        $config->heartbeat((int) $connectionConfiguration->heartbeatInterval());
        $config->qosCount($qoSConfiguration->count);
        $config->qosSize($qoSConfiguration->size);
        $config->qosGlobal($qoSConfiguration->global);

        return $config;
    }
}
