<?php

namespace inisire\mqtt;

use BinSoul\Net\Mqtt as MQTT;
use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use inisire\fibers\Broadcast;
use inisire\fibers\Contract\SocketFactory;
use inisire\fibers\Network\Socket;
use inisire\fibers\Promise;
use inisire\fibers\Scheduler;
use inisire\mqtt\Contract\MessageHandler;
use inisire\Protocol\MiIO\Contract\Packet;
use Psr\Log\LoggerInterface;


class Connection implements EventEmitterInterface
{
    use EventEmitterTrait;

    private MQTT\PacketFactory $factory;

    private MQTT\PacketIdentifierGenerator $identifierGenerator;

    private MQTT\PacketStream $buffer;

    private bool $connected = false;

    private ?Socket $socket = null;

    /**
     * @var array<MessageHandler>
     */
    private array $messageHandlers = [];

    /**
     * @var array<MQTT\Flow>
     */
    private array $flows = [];

    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly SocketFactory $socketFactory
    )
    {
        $this->factory = new MQTT\DefaultPacketFactory();
        $this->buffer = new MQTT\PacketStream();
        $this->identifierGenerator = new MQTT\DefaultIdentifierGenerator();
    }

    public function connect(string $host, int $port = 1883, int $timeout = 5): bool
    {
        $this->logger->debug('Connection: in progress');

        $this->socket = $this->socketFactory->createTCP();

        if (!$this->socket->connect($host, $port, $timeout)) {
            return false;
        }

        Scheduler::async(function () {
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

        Scheduler::async(function () use ($connection) {
            while ($this->isConnected()) {
                Scheduler::sleep(floor($connection->getKeepAlive() * 0.75));
                $this->send(new MQTT\Packet\PingRequestPacket());
            }
        });

        $this->logger->debug(sprintf('Connection: %s', $this->connected ? 'connected' : 'not connected'));

        return $this->connected;
    }

    public function registerMessageHandler(MessageHandler $handler): void
    {
        $this->messageHandlers[] = $handler;
    }

    /**
     * @return iterable<MessageHandler>
     */
    public function getMessageHandlers(): iterable
    {
        foreach ($this->messageHandlers as $handler) {
            yield $handler;
        }
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

    public function send(MQTT\Packet $packet): false|int
    {
        $this->logger->debug('Packet send', [
            'packet' => $packet::class
        ]);

        return $this->socket->write($packet->__toString());
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

                foreach ($this->getMessageHandlers() as $handler) {
                    $handler->handleMessage($message);
                }

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
    public function read(): iterable
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
    
    public function isConnected(): bool
    {
        return $this->connected;
    }

    public function __destruct()
    {
        $this->close();
    }
}