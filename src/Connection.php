<?php

namespace inisire\mqtt;

use BinSoul\Net\Mqtt as MQTT;
use inisire\fibers\Broadcast;
use inisire\fibers\Contract\SocketFactory;
use inisire\fibers\Network\Socket;
use inisire\fibers\Scheduler;
use Psr\Log\LoggerInterface;


class Connection
{
    private MQTT\PacketFactory $factory;

    private MQTT\PacketIdentifierGenerator $identifierGenerator;

    private MQTT\PacketStream $buffer;

    private bool $connected = false;

    private ?Socket $socket = null;

    private Broadcast $inbound;

    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly SocketFactory $socketFactory
    )
    {
        $this->factory = new MQTT\DefaultPacketFactory();
        $this->buffer = new MQTT\PacketStream();
        $this->identifierGenerator = new MQTT\DefaultIdentifierGenerator();
        $this->inbound = new Broadcast();
    }

    public function connect(string $host, int $port = 1883, int $timeout = 5): bool
    {
        $this->socket = $this->socketFactory->createTCP();

        if (!$this->socket->connect($host, $port, $timeout)) {
            return false;
        }

        Scheduler::async(function () {
            while ($this->socket->isConnected()) {
                foreach ($this->read() as $packet) {
                    $this->onPacketReceived($packet);
                    $this->inbound->write($packet);
                }
            }
            $this->inbound->close();

            $this->logger->debug('Disconnected');
        });

        $connection = new MQTT\DefaultConnection();

        $request = new MQTT\Packet\ConnectRequestPacket();
        $request->setProtocolLevel($connection->getProtocol());
        $request->setKeepAlive($connection->getKeepAlive());
        $request->setClientID($connection->getClientID());
        $request->setCleanSession($connection->isCleanSession());
        $request->setUsername($connection->getUsername());
        $request->setPassword($connection->getPassword());

        $will = $connection->getWill();
        if ($will !== null && $will->getTopic() !== '' && $will->getPayload() !== '') {
            $request->setWill($will->getTopic(), $will->getPayload(), $will->getQosLevel(), $will->isRetained());
        }

        $this->send($request);

        foreach ($this->inboundPackets() as $packet) {
            if ($packet->getPacketType() === MQTT\Packet::TYPE_CONNACK) {
                $this->connected = true;
                break;
            }
        }

        Scheduler::async(function () use ($connection) {
            while ($this->connected) {
                Scheduler::sleep(floor($connection->getKeepAlive() * 0.75));
                $this->send(new MQTT\Packet\PingRequestPacket());
            }
        });

        $this->logger->debug('Connection: ' . $this->connected ? 'connected' : 'not connected');

        return $this->connected;
    }

    public function subscribe(string $topic, int $qos = 0): bool
    {
        $request = new MQTT\Packet\SubscribeRequestPacket();
        $request->setTopic($topic);
        $request->setQosLevel($qos);
        $request->setIdentifier($this->identifierGenerator->generatePacketIdentifier());

        $this->send($request);

        $subscribed = false;
        foreach ($this->inboundPackets() as $packet) {
            if ($packet instanceof MQTT\Packet\SubscribeResponsePacket && $packet->getIdentifier() === $request->getIdentifier()) {
                $subscribed = true;
                break;
            }
        }

        $this->logger->debug('Subscribe: ' . $subscribed ? 'subscribed' : 'not subscribed');

        return $subscribed;
    }

    private function inboundPackets(): iterable
    {
        $packets = $this->inbound->attach(new \inisire\fibers\Channel());

        return $packets->iterate();
    }

    /**
     * @return iterable<MQTT\Message>
     */
    public function messages(): iterable
    {
        $packets = $this->inbound->attach(new \inisire\fibers\Channel());

        foreach ($packets->iterate() as $packet) {
            if ($packet instanceof MQTT\Packet\PublishRequestPacket) {
                yield new MQTT\DefaultMessage($packet->getTopic(), $packet->getPayload(), $packet->getQosLevel(), $packet->isRetained(), $packet->isDuplicate());
            }
        }
    }

    public function send(MQTT\Packet $packet): false|int
    {
        return $this->socket->write($packet->__toString());
    }

    private function onPacketReceived(MQTT\Packet $packet): void
    {
        switch ($packet->getPacketType()) {
            case MQTT\Packet::TYPE_PUBLISH: {
                if (!$packet instanceof MQTT\Packet\PublishRequestPacket) {
                    break;
                }

                $response = match ($packet->getQosLevel()) {
                    0 => null,
                    1 => new MQTT\Packet\PublishAckPacket(),
                    2 => new MQTT\Packet\PublishReceivedPacket()
                };

                if ($response) {
                    $response->setIdentifier($packet->getIdentifier());
                    $this->send($response);
                }

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
        $this->connected = false;

        if ($this->socket->isConnected()) {
            $this->socket->close();
        }
    }

    public function __destruct()
    {
        $this->close();
    }
}