<?php

namespace Queue\MessageQueue;

use BadMethodCallException;
use InvalidArgumentException;
use Redis;
use function ini_set;
use function time;


class Subscriber extends MessageQueue{

    protected $connection_timeout = 5;
    /**
     * @var int Theoretically, time allowed to process one message
     */
    protected $subscriber_timeout = 60;

    public function __construct(Redis $redis){
        parent::__construct($redis);
        ini_set('default_socket_timeout', -1);
    }

    /**
     * @param int $connection_timeout
     *
     * @throws InvalidArgumentException
     */
    public function setConnectionTimeout(int $connection_timeout){
        if($connection_timeout < 1){
            throw new InvalidArgumentException('Timeout cannot be less than 1 second');
        }

        $this->connection_timeout = $connection_timeout;
    }

    /**
     * @throws BadMethodCallException
     *
     * @return false|string
     */
    public function sub(){
        if($this->paused()){
            return false;
        }

        $this->register();

        return $this->redis->brpoplpush($this->pending_queue, $this->processing_queue, $this->connection_timeout);
    }

    private function register(): void{
        $this->redis->zAdd($this->subscribers, time() + $this->subscriber_timeout, $this->process_id);
    }

    /**
     * @param $message
     *
     * @throws BadMethodCallException
     * @return int
     */
    public function reQueue($message): int{
        if($this->ack($message) > 0){
            return $this->redis->lPush($this->pending_queue, $message);
        }

        return 0;
    }

    /**
     * @param $message
     *
     * @throws BadMethodCallException
     *
     * @return int
     */
    public function ack($message): int{
        $this->checkName();

        return $this->redis->lRem($this->processing_queue, $message, -1);
    }
}