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

use function Amp\asyncCall;
use function Amp\call;
use Amp\Promise;
use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Message;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ServiceBus\Transport\Amqp\AmqpQueue;

/**
 * @internal
 */
final class PhpInnacleConsumer
{
    private Channel $channel;

    /**
     * Listen queue.
     */
    private AmqpQueue $queue;

    private LoggerInterface $logger;

    /**
     * Consumer tag.
     */
    private ?string $tag;

    public function __construct(AmqpQueue $queue, Channel $channel, ?LoggerInterface $logger = null)
    {
        /** @noinspection PhpUnhandledExceptionInspection */
        $this->tag = \sha1((string) \random_bytes(16));

        $this->queue   = $queue;
        $this->channel = $channel;
        $this->logger  = $logger ?? new NullLogger();
    }

    /**
     * Listen queue messages.
     *
     * @psalm-param callable(PhpInnacleIncomingPackage):\Generator $onMessageReceived
     *
     * @return Promise It does not return any result
     */
    public function listen(callable $onMessageReceived): Promise
    {
        $this->logger->info('Creates new consumer on channel for queue "{queue}" with tag "{consumerTag}"', [
            'queue'       => $this->queue->name,
            'consumerTag' => $this->tag,
        ]);

        return $this->channel->consume(
            $this->createMessageHandler($onMessageReceived),
            $this->queue->name,
            (string) $this->tag,
            false,
            false,
            false,
            true
        );
    }

    /**
     * Stop watching the queue.
     *
     * @return Promise It does not return any result
     */
    public function stop(): Promise
    {
        return call(
            function(): \Generator
            {
                if (true === isset($this->tag))
                {
                    yield $this->channel->cancel($this->tag);
                }

                $this->logger->info(
                    'Subscription canceled',
                    [
                        'queue'       => $this->queue->name,
                        'consumerTag' => $this->tag,
                    ]
                );
            }
        );
    }

    /**
     * Create listener callback.
     *
     * @psalm-param callable(PhpInnacleIncomingPackage):\Generator $onMessageReceived
     *
     * @psalm-return callable(Message, Channel):void
     */
    private function createMessageHandler($onMessageReceived): callable
    {
        return function(Message $message, Channel $channel) use ($onMessageReceived): void
        {
            try
            {
                $incomingPackage = new PhpInnacleIncomingPackage($message, $channel);

                $this->logger->debug('New message received from "{queueName}"', [
                    'queueName'         => $this->queue->name,
                    'packageId'         => $incomingPackage->id(),
                    'traceId'           => $incomingPackage->traceId(),
                    'rawMessagePayload' => $incomingPackage->payload(),
                    'rawMessageHeaders' => $incomingPackage->headers(),
                ]);

                asyncCall($onMessageReceived, $incomingPackage);

                unset($incomingPackage);
            }
            catch (\Throwable $throwable)
            {
                $this->logger->error(
                    'Error occurred: {throwableMessage}',
                    [
                        'throwableMessage'  => $throwable->getMessage(),
                        'throwablePoint'    => \sprintf('%s:%d', $throwable->getFile(), $throwable->getLine()),
                        'rawMessagePayload' => $message->content(),
                    ]
                );
            }
        };
    }
}
