<?php

namespace inisire\mqtt;

use BinSoul\Net\Mqtt as MQTT;
use Evenement\EventEmitterTrait;
use inisire\fibers\Network\Exception\ConnectionException;
use inisire\fibers\Network\Exception\Timeout;
use inisire\fibers\Network\TCP\Connector;
use inisire\fibers\Network\TCP\Socket;
use inisire\fibers\Promise;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use function inisire\fibers\async;


class Connection implements LoggerAwareInterface
{
    use EventEmitterTrait;

    private ?Socket $socket = null;

    private bool $connected = false;

    private MQTT\PacketFactory $factory;

    private MQTT\PacketIdentifierGenerator $identifierGenerator;

    private MQTT\PacketStream $buffer;

    /**
     * @var array<MQTT\Flow>
     */
    private array $flows = [];

    private LoggerInterface $logger;

    private Connector $connector;

    public function __construct()
    {
        echo 'Connection #' . spl_object_hash($this) . ' created' . PHP_EOL;

        $this->factory = new MQTT\DefaultPacketFactory();
        $this->buffer = new MQTT\PacketStream();
        $this->identifierGenerator = new MQTT\DefaultIdentifierGenerator();
        $this->connector = new Connector();
        $this->logger = new NullLogger();
    }

    public function connect(string $host, int $port = 1883, int $timeout = 10): bool
    {
        $this->logger->debug('MQTT\Connection: connection in progress');

        try {
            $this->socket = $this->connector->connect($host, $port, $timeout);
            $this->socket->onData([$this, 'handleSocketData']);
            $this->socket->onClose([$this, 'handleClose']);
        } catch (ConnectionException $exception) {
            $this->logger->error('MQTT\Connection: socket error', ['code' => $exception->getCode(), 'error' => $exception->getMessage()]);
            return false;
        }

        $connection = new MQTT\DefaultConnection();
        $flow = new Flow(new MQTT\Flow\OutgoingConnectFlow($this->factory, $connection, $this->identifierGenerator), new Promise());
        $this->flows[] = $flow;
        $this->send($flow->start());
        $this->connected = $flow->await(new Promise\Timeout($timeout, false));

        async(function () use ($connection) {
            while ($this->isConnected()) {
                \inisire\fibers\asleep($connection->getKeepAlive() * 0.75);

                $flow = new Flow(new MQTT\Flow\OutgoingPingFlow($this->factory), new Promise());
                $this->flows[] = $flow;
                $this->send($flow->start());

                $success = $flow->await(new Promise\Timeout(5.0, false));

                if ($success === false) {
                    $this->close();
                    break;
                }
            }
        });

        $this->logger->debug(sprintf('MQTT\Connection: %s', $this->connected ? 'connected' : 'not connected'));

        if ($this->connected) {
            $this->emit('connected');
        }

        return $this->connected;
    }

    public function handleClose(): void
    {
        $this->close();
    }

    public function onConnected(callable $handler): void
    {
        $this->on('connected', $handler);
    }

    public function subscribe(MQTT\Subscription $subscription): bool
    {
        $this->logger->debug('Subscribe', ['filter' => $subscription->getFilter()]);

        $flow = new Flow(new MQTT\Flow\OutgoingSubscribeFlow($this->factory, [$subscription], $this->identifierGenerator), new Promise());
        $this->flows[] = $flow;
        $this->send($flow->start());
        $subscribed = $flow->await(new Promise\Timeout(5.0, false));

        $this->logger->debug('Subscribed');

        return $subscribed;
    }
    
    public function publish(MQTT\Message $message): bool
    {
        $this->logger->debug('Publish', [
            'topic' => $message->getTopic(),
            'payload' => $message->getPayload(),
            'qos' => $message->getQosLevel()
        ]);

        $flow = new Flow(new MQTT\Flow\OutgoingPublishFlow($this->factory, $message, $this->identifierGenerator), new Promise());
        $this->flows[] = $flow;
        $this->send($flow->start());
        $published = $flow->await(new Promise\Timeout(5.0, false));

        $this->logger->debug('Published');

        return $published;
    }

    private function send(MQTT\Packet $packet): false|int
    {
        $this->logger->debug('Packet send', [
            'packet' => $packet::class
        ]);

        try {
            $result = $this->socket->write($packet->__toString());
        } catch (ConnectionException $exception) {
            $this->logger->error('MQTT\Connection: socket write error', ['code' => $exception->getCode(), 'error' => $exception->getMessage()]);
            $this->close();
            return false;
        }

        return $result;
    }

    private function onPacketReceived(MQTT\Packet $packet): void
    {
        $this->logger->debug('Packet received', [
            'packet' => $packet::class,
            'flows' => count($this->flows)
        ]);

        foreach ($this->flows as $id => $flow) {
            if ($flow->accept($packet)) {
                if ($next = $flow->next($packet)) {
                    $this->send($next);
                }
            }

            if ($flow->isFinished()) {
                unset($this->flows[$id]);
            }
        }

        switch ($packet->getPacketType()) {
            case MQTT\Packet::TYPE_PUBLISH: {
                if (!$packet instanceof MQTT\Packet\PublishRequestPacket) {
                    break;
                }

                $message = new MQTT\DefaultMessage(
                    $packet->getTopic(),
                    $packet->getPayload(),
                    $packet->getQosLevel(),
                    $packet->isRetained(),
                    $packet->isDuplicate()
                );

                $this->emit('message', [$message]);

                break;
            }

            case MQTT\Packet::TYPE_DISCONNECT: {
                $this->close();
            }
        }
    }

    /**
     * @throws MQTT\Exception\EndOfStreamException
     */
    public function handleSocketData(string $data): void
    {
        $this->buffer->write($data);

        while ($this->buffer->getRemainingBytes() > 0) {
            $type = $this->buffer->readByte() >> 4;

            try {
                $packet = $this->factory->build($type);
            } catch (MQTT\Exception\UnknownPacketTypeException $e) {
                $this->logger->error($e->getMessage());
                continue;
            }

            $this->buffer->seek(-1);
            $position = $this->buffer->getPosition();

            try {
                $packet->read($this->buffer);
                $this->buffer->cut();
                $this->onPacketReceived($packet);
            } catch (MQTT\Exception\EndOfStreamException $e) {
                $this->buffer->setPosition($position);
                break;
            } catch (MQTT\Exception\MalformedPacketException $e) {
                $this->logger->error($e->getMessage());
                continue;
            }
        }
    }

    public function close()
    {
        if (!$this->connected) {
            return;
        }

        $this->connected = false;
        $this->socket?->close();
        $this->flows = [];

        $this->logger->info('Connection closed');

        $this->emit('close');
        $this->removeAllListeners();
    }

    public function onDisconnect(callable $handler): void
    {
        $this->on('close', $handler);
    }
    
    public function isConnected(): bool
    {
        return $this->connected;
    }

    public function __destruct()
    {
        echo 'Connection #' . spl_object_hash($this) . ' destroyed' . PHP_EOL;
    }

    public function onMessage(callable $handler): void
    {
        $this->on('message', $handler);
    }

    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }
}