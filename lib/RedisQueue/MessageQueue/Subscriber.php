<?php

namespace RedisQueue\MessageQueue;

use BadMethodCallException;
use function time;


class Subscriber extends MessageQueue{

    protected $connection_timeout = 5;
    protected $last_message;

    /**
     * @param int $connection_timeout A timeout of zero can be used to wait indefinitely
     *                                Note that while waiting, the subscriber may time out from the register.
     *                                You can adjust accordingly by increasing the subscriber_timeout
     *                                Also, remember to set default_socket_timeout = -1 before connecting to redis
     */
    public function setConnectionTimeout(int $connection_timeout){
        if($connection_timeout < 0){
            $connection_timeout = 0;
        }

        $this->connection_timeout = $connection_timeout;
    }

    /**
     * @throws BadMethodCallException
     *
     * @return false|mixed
     */
    public function sub(){
        if($this->paused()){
            return false;
        }

        $this->register();

        return $this->last_message = $this->redis->brpoplpush($this->pending_queue, $this->processing_queue,
                                                              $this->connection_timeout);
    }

    private function register(): void{
        $this->redis->zAdd($this->subscribers, time() + $this->subscriber_timeout, $this->process_id);
    }

    /**
     * @throws BadMethodCallException
     *
     * @return int
     */
    public function reQueue(): int{
        if($this->ack() > 0){
            return $this->redis->lPush($this->pending_queue, $this->last_message);
        }

        return 0;
    }

    /**
     * @throws BadMethodCallException
     *
     * @return int
     */
    public function ack(): int{
        $this->checkName();

        return $this->redis->lRem($this->processing_queue, $this->last_message, -1);
    }
}