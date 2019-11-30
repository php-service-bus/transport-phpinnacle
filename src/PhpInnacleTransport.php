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
     * Client for work with AMQP protocol.
     */
    private Client $client;

    /**
     * Null if not connected.
     */
    private ?Channel $channel;

    private ?PhpInnaclePublisher $publisher;

    private LoggerInterface $logger;

    /**
     * @psalm-var array<string, \ServiceBus\Transport\PhpInnacle\PhpInnacleConsumer>
     *
     * @var \ServiceBus\Transport\PhpInnacle\PhpInnacleConsumer[]
     */
    private array

 $consumers = [];

    private Config $config;

    public function __construct(
        AmqpConnectionConfiguration $connectionConfig,
        AmqpQoSConfiguration $qosConfig = null,
        ?LoggerInterface $logger = null
    ) {
        $qosConfig = $qosConfig ?? new AmqpQoSConfiguration();

        $this->logger = $logger ?? new NullLogger();
        $this->config = $this->adaptConfig($connectionConfig, $qosConfig);

        $this->client = new Client($this->config);
    }

    /**
     * {@inheritdoc}
     */
    public function connect(): Promise
    {
        return call(
            function(): \Generator
            {
                if (true === $this->client->isConnected())
                {
                    return;
                }

                try
                {
                    yield $this->client->connect();

                    /** @var Channel $channel */
                    $channel = yield $this->client->channel();

                    $this->channel = $channel;

                    $this->logger->info('Connected to broker', [
                        'host'  => $this->config->host(),
                        'port'  => $this->config->port(),
                        'vhost' => $this->config->vhost(),
                    ]);
                }
                catch (\Throwable $throwable)
                {
                    throw new ConnectionFail(
                        \sprintf(
                            'Can\'t connect to %s:%d (vhost: %s) with credentials %s:%s',
                            $this->config->host(),
                            $this->config->port(),
                            $this->config->vhost(),
                            $this->config->user(),
                            $this->config->password()
                        ),
                        (int) $throwable->getCode(),
                        $throwable
                    );
                }
            }
        );
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect(): Promise
    {
        return call(
            function(): \Generator
            {
                try
                {
                    if (true === $this->client->isConnected())
                    {
                        yield $this->client->disconnect();
                    }
                }
                catch (\Throwable $throwable)
                {
                    /** Not interested */
                }

                $this->logger->info('Disconnect from broker', [
                    'host'  => $this->config->host(),
                    'port'  => $this->config->port(),
                    'vhost' => $this->config->vhost(),
                ]);
            }
        );
    }

    /**
     * @psalm-suppress MixedTypeCoercion
     *
     * {@inheritdoc}
     */
    public function consume(callable $onMessage, Queue ...$queues): Promise
    {
        return call(
            function(array $queues) use ($onMessage): \Generator
            {
                yield $this->connect();

                /** @var Channel $channel */
                $channel = yield $this->client->channel();

                /** @var AmqpQueue $queue */
                foreach ($queues as $queue)
                {
                    $this->logger->info('Starting a subscription to the "{queueName}" queue', [
                        'host'      => $this->config->host(),
                        'port'      => $this->config->port(),
                        'vhost'     => $this->config->vhost(),
                        'queueName' => $queue->name,
                        'channel'   => $channel->id(),
                    ]);

                    $consumer = new PhpInnacleConsumer($queue, $channel, $this->logger);

                    $consumer->listen($onMessage);

                    $this->consumers[$queue->name] = $consumer;
                }
            },
            $queues
        );
    }

    /**
     * {@inheritdoc}
     */
    public function stop(): Promise
    {
        return call(
            function(array $consumers): \Generator
            {
                /**
                 * @var string             $queueName
                 * @var PhpInnacleConsumer $consumer
                 */
                foreach ($consumers as $queueName => $consumer)
                {
                    $this->logger->info('Completing the subscription to the "{queueName}" queue', [
                        'host'      => $this->config->host(),
                        'port'      => $this->config->port(),
                        'vhost'     => $this->config->vhost(),
                        'queueName' => $queueName,
                    ]);

                    if (true === isset($this->consumers[$queueName]))
                    {
                        /** @var PhpInnacleConsumer $consumer */
                        $consumer = $this->consumers[$queueName];

                        yield $consumer->stop();

                        unset($this->consumers[$queueName]);
                    }
                }
            },
            $this->consumers
        );
    }

    /**
     * {@inheritdoc}
     */
    public function send(OutboundPackage $outboundPackage): Promise
    {
        return call(
            function(OutboundPackage $outboundPackage): \Generator
            {
                yield $this->connect();

                /** @var Channel $channel */
                $channel = $this->channel;

                if (false === isset($this->publisher))
                {
                    $this->publisher = new PhpInnaclePublisher($channel, $this->logger);
                }

                yield $this->publisher->process($outboundPackage);
            },
            $outboundPackage
        );
    }

    /**
     * {@inheritdoc}
     */
    public function createTopic(Topic $topic, TopicBind ...$binds): Promise
    {
        /** @var AmqpExchange $topic */

        return call(
            function(AmqpExchange $exchange, array $binds): \Generator
            {
                /**
                 * @var \ServiceBus\Transport\Common\TopicBind[] $binds
                 * @psalm-var array<mixed, \ServiceBus\Transport\Common\TopicBind> $binds
                 */
                yield $this->connect();

                /** @var Channel $channel */
                $channel = $this->channel;

                $configurator = new PhpInnacleConfigurator($channel);

                yield $configurator->doCreateExchange($exchange);
                yield $configurator->doBindExchange($exchange, $binds);
            },
            $topic,
            $binds
        );
    }

    /**
     * {@inheritdoc}
     */
    public function createQueue(Queue $queue, QueueBind ...$binds): Promise
    {
        /** @var AmqpQueue $queue */

        return call(
            function(AmqpQueue $queue, array $binds): \Generator
            {
                /**
                 * @var \ServiceBus\Transport\Common\QueueBind[] $binds
                 * @psalm-var array<mixed, \ServiceBus\Transport\Common\QueueBind> $binds
                 */
                yield $this->connect();

                /** @var Channel $channel */
                $channel = $this->channel;

                $configurator = new PhpInnacleConfigurator($channel);

                yield $configurator->doCreateQueue($queue);
                yield $configurator->doBindQueue($queue, $binds);
            },
            $queue,
            $binds
        );
    }

    private function adaptConfig(
        AmqpConnectionConfiguration $connectionConfiguration,
        AmqpQoSConfiguration $qoSConfiguration
    ): Config {
        $config = new Config(
            $connectionConfiguration->host(),
            $connectionConfiguration->port(),
            $connectionConfiguration->user(),
            $connectionConfiguration->password(),
            $connectionConfiguration->virtualHost()
        );

        $timeout = 0 < $connectionConfiguration->timeout()
            ? (int) $connectionConfiguration->timeout()
            : 1;

        $config->timeout($timeout);

        $config->heartbeat((int) $connectionConfiguration->heartbeatInterval());
        $config->qosCount($qoSConfiguration->count);
        $config->qosSize($qoSConfiguration->size);
        $config->qosGlobal($qoSConfiguration->global);

        return $config;
    }
}
