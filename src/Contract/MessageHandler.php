<?php

namespace inisire\mqtt\Contract;

use BinSoul\Net\Mqtt as MQTT;

interface MessageHandler
{
    public function handleMessage(MQTT\Message $message): void;
}