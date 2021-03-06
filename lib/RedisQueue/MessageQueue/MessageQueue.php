<?php

namespace RedisQueue\MessageQueue;

use BadMethodCallException;
use InvalidArgumentException;
use Redis;
use UnexpectedValueException;
use function bin2hex;
use function random_bytes;
use function str_ireplace;
use function time;
use function trim;

abstract class MessageQueue{

    /**
     * @var Redis
     */
    protected $redis;
    protected $namespace;
    protected $name;
    protected $pending_queue;
    protected $processing_queue;
    protected $shutdown_key;
    protected $subscribers;
    protected $process_id;
    protected $subscriber_timeout = 60;

    /**
     * MessageQueue constructor.
     *
     * @param Redis $redis
     *
     * @throws \Exception
     */
    public function __construct(Redis $redis){
        $this->namespace = str_ireplace('\\', ':', __NAMESPACE__);
        $this->redis = $redis;

        $this->process_id = bin2hex(random_bytes(5));
    }

    /**
     * @return string
     */
    public function getProcessID(): string{
        return $this->process_id;
    }

    /**
     * @param int $subscriber_timeout Theoretically, time allowed to process one message
     *
     * @throws InvalidArgumentException
     */
    public function setSubscriberTimeout(int $subscriber_timeout): void{
        if($subscriber_timeout < 1){
            throw new InvalidArgumentException('Timeout cannot be less than 1 second');
        }

        $this->subscriber_timeout = $subscriber_timeout;
    }

    /**
     * @param string $name Can contain namespaces
     *
     * @throws UnexpectedValueException
     */
    public function setName(string $name): void{
        $this->name = trim($name);
        $this->pending_queue = $this->namespace . ':' . $this->name . ':' . 'pending';
        $this->processing_queue = $this->namespace . ':' . $this->name . ':' . 'processing';

        $this->subscribers = $this->namespace . ':' . $this->name . ':subscribers';
        $this->shutdown_key = $this->namespace . ':' . $this->name . ':shutdown';

        if($this->getPendingSize() === false || $this->getProcessingSize() === false
           || $this->getSubscriberSize() === false){
            $this->pending_queue = $this->processing_queue = $this->shutdown_key = $this->subscribers = null;
            throw new UnexpectedValueException('Invalid queue name. Not a list or set');
        }

        $this->cleanUpSubscribers();
    }

    /**
     * @return int|false
     */
    public function getPendingSize(){
        if($this->pending_queue !== null){
            return $this->redis->lLen($this->pending_queue);
        }

        return false;
    }

    /**
     * @return int|false
     */
    public function getProcessingSize(){
        if($this->processing_queue !== null){
            return $this->redis->lLen($this->processing_queue);
        }

        return false;
    }

    /**
     * @return int|false
     */
    public function getSubscriberSize(){
        if($this->subscribers !== null){
            return $this->redis->zCount($this->subscribers, time(), '+inf');
        }

        return false;
    }

    private function cleanUpSubscribers(): int{
        return $this->redis->zRemRangeByScore($this->subscribers, '-inf', time() - 1);
    }

    /**
     * @throws BadMethodCallException
     *
     * @return bool
     */
    public function paused(): bool{
        $this->checkName();

        return (bool)$this->redis->get($this->shutdown_key);
    }

    /**
     * @throws BadMethodCallException
     */
    protected function checkName(): void{
        if($this->pending_queue === null){
            throw new BadMethodCallException('Queue name not set');
        }
    }
}