<?php

/**
 * PHPinnacle RabbitMQ adapter.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Transport\PhpInnacle\Tests;

use function ServiceBus\Common\readReflectionPropertyValue;
use function ServiceBus\Common\uuid;
use Amp\Loop;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;
use ServiceBus\Transport\Amqp\AmqpConnectionConfiguration;
use ServiceBus\Transport\Amqp\AmqpExchange;
use ServiceBus\Transport\Amqp\AmqpQueue;
use ServiceBus\Transport\Amqp\AmqpTransportLevelDestination;
use ServiceBus\Transport\Common\Package\OutboundPackage;
use ServiceBus\Transport\Common\QueueBind;
use ServiceBus\Transport\Common\TopicBind;
use ServiceBus\Transport\PhpInnacle\PhpInnacleIncomingPackage;
use ServiceBus\Transport\PhpInnacle\PhpInnacleTransport;

/**
 *
 */
final class PhpInnacleTransportTest extends TestCase
{
    /**
     * @var PhpInnacleTransport
     */
    private $transport;

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->transport = new PhpInnacleTransport(
            new AmqpConnectionConfiguration((string) \getenv('TRANSPORT_CONNECTION_DSN'))
        );
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function tearDown(): void
    {
        parent::tearDown();

        Loop::run(
            function(): void
            {
                $this->transport->connect()->onResolve(
                    function(?\Throwable $throwable): \Generator
                    {
                        if (null !== $throwable)
                        {
                            static::fail($throwable->getMessage());

                            return;
                        }

                        /** @var \PHPinnacle\Ridge\Channel|null $channel */
                        $channel = readReflectionPropertyValue($this->transport, 'channel');

                        if (null !== $channel)
                        {
                            yield $channel->exchangeDelete('createExchange');
                            yield  $channel->queueDelete('createQueue');

                            yield $channel->exchangeDelete('createExchange2');
                            yield $channel->queueDelete('createQueue2');

                            yield $channel->exchangeDelete('consume');
                            yield $channel->queueDelete('consume.messages');

                            yield  $this->transport->disconnect();
                        }
                    }
                );
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function connect(): void
    {
        Loop::run(
            function(): void
            {
                $this->transport->connect()->onResolve(
                    function(?\Throwable $throwable): \Generator
                    {
                        if (null !== $throwable)
                        {
                            static::fail($throwable->getMessage());
                        }

                        yield $this->transport->disconnect();
                    }
                );
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function createExchange(): void
    {
        Loop::run(
            function(): void
            {
                $this->transport->createTopic(AmqpExchange::topic('createExchange'))->onResolve(
                    function(?\Throwable $throwable): \Generator
                    {
                        if (null !== $throwable)
                        {
                            static::fail($throwable->getMessage());
                        }

                        yield $this->transport->disconnect();
                    }
                );
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function createQueue(): void
    {
        Loop::run(
            function(): void
            {
                $this->transport->createQueue(AmqpQueue::default('createQueue'))->onResolve(
                    function(?\Throwable $throwable): \Generator
                    {
                        if (null !== $throwable)
                        {
                            static::fail($throwable->getMessage());
                        }

                        yield $this->transport->disconnect();
                    }
                );
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function bindTopic(): void
    {
        Loop::run(
            function(): void
            {
                $promise = $this->transport->createTopic(
                    AmqpExchange::topic('createExchange'),
                    TopicBind::create(
                        AmqpExchange::topic('createExchange2'),
                        'qwerty'
                    )
                );

                $promise->onResolve(
                    function(?\Throwable $throwable): \Generator
                    {
                        if (null !== $throwable)
                        {
                            static::fail($throwable->getMessage());
                        }

                        yield $this->transport->disconnect();
                    }
                );
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function bindQueue(): void
    {
        Loop::run(
            function(): void
            {
                $promise = $this->transport->createQueue(
                    AmqpQueue::default('createQueue'),
                    QueueBind::create(
                        AmqpExchange::topic('createExchange2'),
                        'qwerty'
                    )
                );

                $promise->onResolve(
                    function(?\Throwable $throwable): \Generator
                    {
                        if (null !== $throwable)
                        {
                            static::fail($throwable->getMessage());
                        }

                        yield $this->transport->disconnect();
                    }
                );
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function consume(): void
    {
        Loop::run(
            function(): \Generator
            {
                $exchange = AmqpExchange::direct('consume');
                $queue    = AmqpQueue::default('consume.messages');

                yield $this->transport->createTopic($exchange);
                yield $this->transport->createQueue($queue, QueueBind::create($exchange, 'consume'));

                yield $this->transport->send(
                    new  OutboundPackage(
                        'somePayload',
                        ['key' => 'value'],
                        new AmqpTransportLevelDestination('consume', 'consume'),
                        uuid()
                    )
                );

                yield $this->transport->consume(
                    function(PhpInnacleIncomingPackage $package): \Generator
                    {
                        static::assertInstanceOf(PhpInnacleIncomingPackage::class, $package);
                        static::assertSame('somePayload', $package->payload());
                        static::assertCount(2, $package->headers());
                        static::assertTrue(Uuid::isValid($package->traceId()));

                        yield $this->transport->disconnect();
                    },
                    $queue
                );
            }
        );
    }
}
