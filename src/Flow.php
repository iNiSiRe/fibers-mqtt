<?php

namespace inisire\mqtt;

use BinSoul\Net\Mqtt as MQTT;
use BinSoul\Net\Mqtt\Packet;
use inisire\fibers\Promise;
use inisire\fibers\Scheduler;

class Flow implements MQTT\Flow
{
    public function __construct(
        private readonly MQTT\Flow $flow,
        private readonly Promise $promise
    )
    {
    }

    public function getCode(): string
    {
        return $this->flow->getCode();
    }

    public function start(): ?Packet
    {
        $start = $this->flow->start();
        
        if ($this->flow->isFinished()) {
            $this->promise->resolve($this->flow->isSuccess());
        }
        
        return $start;
    }

    public function accept(Packet $packet): bool
    {
        return $this->flow->accept($packet);
    }

    public function next(Packet $packet): ?Packet
    {
        $packet = $this->flow->next($packet);

        if ($this->flow->isFinished()) {
            $this->promise->resolve($this->flow->isSuccess());
        }
        
        return $packet;
    }

    public function isFinished(): bool
    {
        return $this->flow->isFinished();
    }

    public function isSuccess(): bool
    {
        return $this->flow->isSuccess();
    }
    
    public function getResult()
    {
        return $this->flow->getResult();
    }

    public function getErrorMessage(): string
    {
        return $this->flow->getErrorMessage();
    }
    
    public function await(?Promise\Timeout $timeout = null): mixed
    {
        return $this->promise->await($timeout);
    }
}