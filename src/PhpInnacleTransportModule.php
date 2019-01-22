<?php

/**
 * phpinnacle transport module for PHP Service Bus
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Transport\Amqp\PhpInnacle;

use ServiceBus\Common\Module\ServiceBusModule;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;

/**
 * @api
 */
final class PhpInnacleTransportModule implements ServiceBusModule
{
    /**
     * @var string
     */
    private $connectionDSN;

    /**
     * @var string
     */
    private $defaultDestinationExchange;

    /**
     * @var string|null
     */
    private $defaultDestinationRoutingKey;

    /**
     * @var int|null
     */
    private $qosSize;

    /**
     * @var int|null
     */
    private $qosCount;

    /**
     * @var bool|null
     */
    private $qosGlobal;

    /**
     * @param string      $connectionDSN
     * @param string      $defaultDestinationExchange
     * @param string|null $defaultDestinationRoutingKey
     */
    public function __construct(string $connectionDSN, string $defaultDestinationExchange, ?string $defaultDestinationRoutingKey)
    {
        $this->connectionDSN                = $connectionDSN;
        $this->defaultDestinationExchange   = $defaultDestinationExchange;
        $this->defaultDestinationRoutingKey = $defaultDestinationRoutingKey;
    }

    /**
     * Apply Quality Of Service settings
     *
     * @param int|null  $size
     * @param int|null  $count
     * @param bool|null $isGlobal
     *
     * @return void
     */
    public function configureQos(?int $size = null, ?int $count = null, ?bool $isGlobal = null): void
    {
        $this->qosSize   = $size;
        $this->qosCount  = $count;
        $this->qosGlobal = $isGlobal;
    }

    /**
     * @inheritdoc
     */
    public function boot(ContainerBuilder $containerBuilder): void
    {
        $loader = new YamlFileLoader($containerBuilder, new FileLocator());
        $loader->load(__DIR__ . '/module.yaml');

        $this->injectParameters($containerBuilder);
    }

    /**
     * Push parameters to container
     *
     * @param ContainerBuilder $containerBuilder
     *
     * @return void
     */
    private function injectParameters(ContainerBuilder $containerBuilder): void
    {
        $parameters = [
            'service_bus.transport.amqp.dsn'                       => $this->connectionDSN,
            'service_bus.transport.amqp.qos_size'                  => $this->qosSize,
            'service_bus.transport.amqp.qos_count'                 => $this->qosCount,
            'service_bus.transport.amqp.qos_global'                => $this->qosGlobal,
            'service_bus.transport.amqp.default_destination_topic' => $this->defaultDestinationExchange,
            'service_bus.transport.amqp.default_destination_key'   => $this->defaultDestinationRoutingKey,
        ];

        foreach($parameters as $key => $value)
        {
            $containerBuilder->setParameter($key, $value);
        }
    }
}
