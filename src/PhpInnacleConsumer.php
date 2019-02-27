<?php /** @noinspection PhpUnhandledExceptionInspection */

/**
 * phpinnacle RabbitMQ adapter.
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
     * Listen queue.
     *
     * @var AmqpQueue
     */
    private $queue;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * Consumer tag.
     *
     * @var string|null
     */
    private $tag;

    /**
     * @noinspection PhpDocMissingThrowsInspection
     *
     * @param AmqpQueue            $queue
     * @param Channel              $channel
     * @param LoggerInterface|null $logger
     */
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
     * @param callable(PhpInnacleIncomingPackage):\Generator $onMessageReceived
     *
     * @return Promise
     */
    public function listen(callable $onMessageReceived): Promise
    {
        $queueName = (string) $this->queue;

        $this->logger->info('Creates new consumer on channel for queue "{queue}" with tag "{consumerTag}"', [
            'queue'       => $queueName,
            'consumerTag' => $this->tag,
        ]);

        /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
        return $this->channel->consume(
            $this->createMessageHandler($onMessageReceived),
            $queueName,
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
        /**
         * @psalm-suppress MixedTypeCoercion
         * @psalm-suppress InvalidArgument
         */
        return call(
            function(): \Generator
            {
                /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                yield $this->channel->cancel($this->tag);

                $this->logger->info(
                    'Subscription canceled',
                    [
                        'queue'       => (string) $this->queue,
                        'consumerTag' => $this->tag,
                    ]
                );
            }
        );
    }

    /**
     * Create listener callback.
     *
     * @param callable(PhpInnacleIncomingPackage):\Generator $onMessageReceived
     *
     * @return callable(Message, Channel):void
     */
    private function createMessageHandler($onMessageReceived): callable
    {
        return function(Message $message, Channel $channel) use ($onMessageReceived): void
        {
            try
            {
                $incomingPackage = PhpInnacleIncomingPackage::received($message, $channel);

                $this->logger->debug('New message received from "{queueName}"', [
                    'queueName'         => (string) $this->queue,
                    'packageId'         => $incomingPackage->id(),
                    'traceId'           => $incomingPackage->traceId(),
                    'rawMessagePayload' => $incomingPackage->payload(),
                    'rawMessageHeaders' => $incomingPackage->headers(),
                ]);

                /** @psalm-suppress InvalidArgument */
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
