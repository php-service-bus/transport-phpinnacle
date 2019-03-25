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

use function Amp\Promise\wait;
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
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->transport = new PhpInnacleTransport(AmqpConnectionConfiguration::createLocalhost());
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function tearDown(): void
    {
        parent::tearDown();

        /** @var \PHPinnacle\Ridge\Channel|null $channel */
        $channel = readReflectionPropertyValue($this->transport, 'channel');

        if (null !== $channel)
        {
            wait($channel->exchangeDelete('createExchange'));
            wait($channel->queueDelete('createQueue'));

            wait($channel->exchangeDelete('createExchange2'));
            wait($channel->queueDelete('createQueue2'));

            wait($channel->exchangeDelete('consume'));
            wait($channel->queueDelete('consume.messages'));

            wait($this->transport->disconnect());
        }
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
        wait($this->transport->connect());
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
        wait($this->transport->createTopic(AmqpExchange::topic('createExchange')));

        static::assertTrue(true);
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
        wait($this->transport->createQueue(AmqpQueue::default('createQueue')));

        static::assertTrue(true);
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
        wait(
            $this->transport->createTopic(
                AmqpExchange::topic('createExchange'),
                TopicBind::create(
                    AmqpExchange::topic('createExchange2'),
                    'qwerty'
                )
            )
        );

        static::assertTrue(true);
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
        wait(
            $this->transport->createQueue(
                AmqpQueue::default('createQueue'),
                QueueBind::create(
                    AmqpExchange::topic('createExchange2'),
                    'qwerty'
                )
            )
        );

        static::assertTrue(true);
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
        $exchange = AmqpExchange::direct('consume');
        $queue    = AmqpQueue::default('consume.messages');

        wait($this->transport->createTopic($exchange));
        wait($this->transport->createQueue($queue, QueueBind::create($exchange, 'consume')));

        Loop::run(
            function() use ($queue)
            {
                yield $this->transport->send(
                    OutboundPackage::create(
                        'somePayload',
                        ['key' => 'value'],
                        new AmqpTransportLevelDestination('consume', 'consume'),
                        uuid()
                    )
                );

                $this->transport->consume(
                    function(PhpInnacleIncomingPackage $package): void
                    {
                        static::assertInstanceOf(PhpInnacleIncomingPackage::class, $package);
                        static::assertSame('somePayload', $package->payload());
                        static::assertCount(2, $package->headers());
                        static::assertTrue(Uuid::isValid($package->traceId()));

                        Loop::stop();
                    },
                    $queue
                );
            }
        );
    }
}
