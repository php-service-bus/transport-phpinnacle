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
use ServiceBus\Transport\Common\Package\OutboundPackage;
use ServiceBus\Transport\Common\Transport;

/**
 * @internal
 */
final class PhpInnaclePublisher
{
    private const AMQP_DURABLE = 2;

    /**
     * @var Channel
     */
    private $channel;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @param Channel         $channel
     * @param LoggerInterface $logger
     */
    public function __construct(Channel $channel, LoggerInterface $logger)
    {
        $this->channel = $channel;
        $this->logger  = $logger;
    }

    /**
     * Send message to broker.
     *
     * @param OutboundPackage $outboundPackage
     *
     * @return Promise
     */
    public function process(OutboundPackage $outboundPackage): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
            function(OutboundPackage $outboundPackage): \Generator
            {
                /** @var Channel $channel */
                $channel = $this->channel;

                $internalHeaders = [
                    'delivery-mode'                     => true === $outboundPackage->persistentFlag ? self::AMQP_DURABLE : null,
                    'expiration'                        => $outboundPackage->expiredAfter,
                    Transport::SERVICE_BUS_TRACE_HEADER => $outboundPackage->traceId,
                ];

                /** @var \ServiceBus\Transport\Amqp\AmqpTransportLevelDestination $destination */
                $destination = $outboundPackage->destination;
                $headers     = \array_filter(\array_merge($internalHeaders, $outboundPackage->headers));
                $content     = $outboundPackage->payload;

                $this->logger->debug('Publish message to "{rabbitMqExchange}" with routing key "{rabbitMqRoutingKey}"', [
                    'traceId'            => $outboundPackage->traceId,
                    'rabbitMqExchange'   => $destination->exchange,
                    'rabbitMqRoutingKey' => $destination->routingKey,
                    'content'            => $content,
                    'headers'            => $headers,
                    'isMandatory'        => $outboundPackage->mandatoryFlag,
                    'isImmediate'        => $outboundPackage->immediateFlag,
                    'expiredAt'          => $outboundPackage->expiredAfter,
                ]);

                yield $channel->publish(
                    $content,
                    $destination->exchange,
                    (string) $destination->routingKey,
                    \array_filter($headers),
                    $outboundPackage->mandatoryFlag,
                    $outboundPackage->immediateFlag
                );
            },
            $outboundPackage
        );
    }
}
