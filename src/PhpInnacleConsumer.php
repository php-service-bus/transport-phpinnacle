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
    /**
     * @var Channel
     */
    private $channel;

    /**
     * Listen queue
     *
     * @var AmqpQueue
     */
    private $queue;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * Consumer tag
     *
     * @var string|null
     */
    private $tag;

    /**
     * @param AmqpQueue            $queue
     * @param Channel              $channel
     * @param LoggerInterface|null $logger
     */
    public function __construct(AmqpQueue $queue, Channel $channel, ?LoggerInterface $logger = null)
    {
        $this->tag = \sha1((string) \microtime(true));

        $this->queue   = $queue;
        $this->channel = $channel;
        $this->logger  = $logger ?? new NullLogger();
    }

    /**
     * Listen queue messages
     *
     * @param callable(PhpInnacleIncomingPackage):\Generator $onMessageReceived
     *
     * @return Promise
     */
    public function listen(callable $onMessageReceived): Promise
    {
        $queueName = (string) $this->queue;
        $logger    = $this->logger;

        $logger->info('Creates new consumer on channel for queue "{queue}" with tag "{consumerTag}"', [
            'queue'       => $queueName,
            'consumerTag' => $this->tag
        ]);

        /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
        return $this->channel->consume(
            self::createMessageHandler($logger, $onMessageReceived), $queueName, (string) $this->tag
        );
    }

    /**
     * Stop watching the queue
     *
     * @return Promise It does not return any result
     */
    public function stop(): Promise
    {
        /**
         * @psalm-suppress MixedTypeCoercion
         * @psalm-suppress InvalidArgument
         */
        return call(
            function(): \Generator
            {
                /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                yield $this->channel->cancel($this->tag);

                $this->logger->info('Subscription canceled', [
                        'queue'       => (string) $this->queue,
                        'consumerTag' => $this->tag
                    ]
                );

            }
        );
    }

    /**
     * Create listener callback
     *
     * @param LoggerInterface $logger
     * @param callable(PhpInnacleIncomingPackage):\Generator $onMessageReceived
     *
     * @return callable(Message, Channel):void
     */
    private static function createMessageHandler(LoggerInterface $logger, $onMessageReceived): callable
    {
        return static function(Message $message, Channel $channel) use ($logger, $onMessageReceived): void
        {
            try
            {
                $incomingPackage = PhpInnacleIncomingPackage::received($message, $channel);

                $logger->debug('New message received', [
                    'packageId'         => $incomingPackage->id(),
                    'traceId'           => $incomingPackage->traceId(),
                    'rawMessagePayload' => $incomingPackage->payload(),
                    'rawMessageHeaders' => $incomingPackage->headers()
                ]);

                /** @psalm-suppress InvalidArgument */
                asyncCall($onMessageReceived, $incomingPackage);

                unset($incomingPackage);
            }
            catch(\Throwable $throwable)
            {
                $logger->error('Error occurred: {throwableMessage}', [
                        'throwableMessage'  => $throwable->getMessage(),
                        'throwablePoint'    => \sprintf('%s:%d', $throwable->getFile(), $throwable->getLine()),
                        'rawMessagePayload' => $message->content()
                    ]
                );
            }
        };
    }
}
