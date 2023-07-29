<?php

namespace inisire\mqtt;

use BinSoul\Net\Mqtt as MQTT;
use Evenement\EventEmitterTrait;
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

    public function __construct()
    {
        $this->factory = new MQTT\DefaultPacketFactory();
        $this->buffer = new MQTT\PacketStream();
        $this->identifierGenerator = new MQTT\DefaultIdentifierGenerator();
        $this->logger = new NullLogger();
    }

    public function connect(string $host, int $port = 1883, int $timeout = 5): bool
    {
        $this->logger->debug('Connection: in progress');

        $this->socket = new Socket();

        if (!$this->socket->connect($host, $port, $timeout)) {
            return false;
        }

        async(function () {
            while ($this->socket->isConnected()) {
                foreach ($this->read() as $packet) {
                    $this->onPacketReceived($packet);
                }
            }
            $this->close();
        });

        $connection = new MQTT\DefaultConnection();
        $flow = new Flow(new MQTT\Flow\OutgoingConnectFlow($this->factory, $connection, $this->identifierGenerator), new Promise());
        $this->flows[] = $flow;
        $this->send($flow->start());
        $this->connected = $flow->await(new Promise\Timeout($timeout, false));

        async(function () use ($connection) {
            while ($this->isConnected()) {
                \inisire\fibers\asleep($connection->getKeepAlive() * 0.75);
                $this->send(new MQTT\Packet\PingRequestPacket());
            }
        });

        $this->logger->debug(sprintf('Connection: %s', $this->connected ? 'connected' : 'not connected'));

        return $this->connected;
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

        $result = $this->socket->write($packet->__toString());

        if ($result === false) {
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
     * @return iterable<MQTT\Packet>
     *
     * @throws MQTT\Exception\EndOfStreamException
     */
    private function read(): iterable
    {
        $data = $this->socket->read();

        if (!$data) {
            return [];
        }

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
                yield $packet;
            } catch (MQTT\Exception\EndOfStreamException $e) {
                $this->buffer->setPosition($position);
                $packet = null;
                break;
            } catch (MQTT\Exception\MalformedPacketException $e) {
                $this->logger->error($e->getMessage());
                $packet = null;
                continue;
            }
        }
    }

    public function close()
    {
        $this->logger->info('Connection closed');

        $this->connected = false;
        $this->flows = [];
        
        if ($this->socket->isConnected()) {
            $this->socket->close();
        }

        $this->emit('end');
    }

    public function onDisconnect(callable $handler): void
    {
        $this->on('end', $handler);
    }
    
    public function isConnected(): bool
    {
        return $this->connected;
    }

    public function onMessage(callable $handler): void
    {
        $this->on('message', $handler);
    }

    public function __destruct()
    {
        $this->close();
    }

    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }
}