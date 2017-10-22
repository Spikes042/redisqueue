<?php

namespace Queue\MessageQueue;

use BadMethodCallException;
use Redis;
use function array_chunk;
use function array_merge;
use function sleep;

class Publisher extends MessageQueue{

    protected $batch_size = 5000;

    public function __construct(Redis $redis){
        parent::__construct($redis);
    }

    /**
     * @param array $messages
     *
     * @throws  BadMethodCallException
     *
     * @return array
     */
    public function pubMultiple(array $messages): array{
        $this->checkName();

        $results = [];

        $batches = array_chunk($messages, $this->batch_size, true);
        foreach($batches as $batch){
            $pipe = $this->redis->multi(Redis::PIPELINE);

            foreach($batch as $message){
                $pipe->lPush($this->pending_queue, $message);
            }

            $r = $pipe->exec();
            $results = array_merge($results, $r);
        }

        return $results;
    }

    /**
     * @param $message
     *
     * @throws  BadMethodCallException
     *
     * @return int
     */
    public function pub($message): int{
        $this->checkName();

        return $this->redis->lPush($this->pending_queue, $message);
    }

    /**
     * Waits till all subscribers are disconnected to make sure a message is not being processed.
     *
     * @throws BadMethodCallException
     * @return int
     */
    public function reQueueAll(): int{
        $this->pause();
        while($this->getSubscriberSize() > 0){
            //Wait until all subscribers are disconnected
            sleep($this->subscriber_timeout / 2);
        }
        $result = 0;

        $script
          = 'local res = redis.call(\'lrange\', KEYS[1], ARGV[1], ARGV[2]);
                redis.call(\'rpush\', KEYS[2], unpack(res));
                return redis.call(\'del\', KEYS[1]);';
        if($this->getProcessingSize() > 0){
            $result = $this->redis->eval($script, [$this->processing_queue, $this->pending_queue, 0, -1], 2);
        }

        $this->resume();

        return $result;
    }

    /**
     * @throws BadMethodCallException
     *
     * @return bool
     */
    public function pause(): bool{
        $this->checkName();

        return $this->redis->set($this->shutdown_key, 1);
    }

    /**
     * @throws BadMethodCallException
     *
     * @return bool
     */
    public function resume(): bool{
        $this->checkName();

        return (bool)$this->redis->del($this->shutdown_key);
    }

}