<?php
namespace Enqueue\LaravelQueue;

use Enqueue\Consumption\ChainExtension;
use Enqueue\Consumption\Context\MessageReceived;
use Enqueue\Consumption\Context\PostMessageReceived;
use Enqueue\Consumption\Context\PreConsume;
use Enqueue\Consumption\Context\PostConsume;
use Enqueue\Consumption\Context\Start;
use Enqueue\Consumption\Extension\LimitConsumedMessagesExtension;
use Enqueue\Consumption\MessageReceivedExtensionInterface;
use Enqueue\Consumption\PostMessageReceivedExtensionInterface;
use Enqueue\Consumption\PreConsumeExtensionInterface;
use Enqueue\Consumption\PostConsumeExtensionInterface;
use Enqueue\Consumption\QueueConsumer;
use Enqueue\Consumption\Result;
use Enqueue\Consumption\StartExtensionInterface;
use Enqueue\LaravelQueue\Queue;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Support\Facades\Log;

class Worker extends \Illuminate\Queue\Worker implements
    StartExtensionInterface,
    PreConsumeExtensionInterface,
    PostConsumeExtensionInterface,
    MessageReceivedExtensionInterface,
    PostMessageReceivedExtensionInterface
{
    protected $connectionName;

    protected $queueNames;

    protected $queue;

    protected $options;

    protected $lastRestart;

    protected $interop = false;

    protected $stopped = false;

    protected $job;

    protected function registerTimeoutHandler($job, WorkerOptions $options)
    {
        // We will register a signal handler for the alarm signal so that we can kill this
        // process if it is running too long because it has frozen. This uses the async
        // signals supported in recent versions of PHP to accomplish it conveniently.
        $timeoutForJob = max($this->timeoutForJob($job, $options), 0);

        if($timeoutForJob > 0 ){
            pcntl_signal(SIGALRM, function () {
                $this->kill(1);
            });
            pcntl_alarm($timeoutForJob);
        }
    }

    public function daemon($connectionName, $queueNames, WorkerOptions $options)
    {
        Log::info("start daemon WorkerOptions variables " , [
            "delay" => $options->delay,
            "memory" => $options->memory,
            "timeout" => $options->timeout,
            "sleep" => $options->sleep,
            "maxTries" => $options->maxTries,
            "force" => $options->force,
            "stopWhenEmpty" => $options->stopWhenEmpty
        ]);
        
        $this->connectionName = $connectionName;
        $this->queueNames = $queueNames;
        $this->options = $options;

        /** @var Queue $queue */
        $this->queue = $this->getManager()->connection($connectionName);
        $this->interop = $this->queue instanceof Queue;

        if (false == $this->interop) {
            parent::daemon($connectionName, $this->queueNames, $options);
        }

        $context = $this->queue->getQueueInteropContext();
        $queueConsumer = new QueueConsumer($context, new ChainExtension([$this]));
        foreach (explode(',', $queueNames) as $queueName) {
            $queueConsumer->bindCallback($queueName, function() {

                $this->runJob($this->job, $this->connectionName, $this->options);
                return Result::ALREADY_ACKNOWLEDGED;
                
            });
        }

        $queueConsumer->consume();
    }

    public function runNextJob($connectionName, $queueNames, WorkerOptions $options)
    {

        $this->connectionName = $connectionName;
        $this->queueNames = $queueNames;
        $this->options = $options;

        /** @var Queue $queue */
        $this->queue = $this->getManager()->connection($connectionName);
        $this->interop = $this->queue instanceof Queue;

        if (false == $this->interop) {
            parent::daemon($connectionName, $this->queueNames, $options);
        }

        $context = $this->queue->getQueueInteropContext();

        $queueConsumer = new QueueConsumer($context, new ChainExtension([
            $this,
            new LimitConsumedMessagesExtension(1),
        ]));

        foreach (explode(',', $queueNames) as $queueName) {
            $queueConsumer->bindCallback($queueName, function() {
            
                $this->runJob($this->job, $this->connectionName, $this->options);

                return Result::ALREADY_ACKNOWLEDGED;
            });
        }

        $queueConsumer->consume();
    }

    public function onStart(Start $context): void
    {
        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals();
        }

        $this->lastRestart = $this->getTimestampOfLastQueueRestart();

        if ($this->stopped) {
            $context->interruptExecution();
        }
    }

    public function onPreConsume(PreConsume $context): void
    {

        if (! $this->daemonShouldRun($this->options, $this->connectionName, $this->queueNames)) {
            $this->pauseWorker($this->options, $this->lastRestart);
        }
        
        $this->stopIfNecessary($this->options, $this->lastRestart, $this->job);

        if ($this->stopped) {
            $context->interruptExecution();
        }
    }

    public function onPostConsume(PostConsume $context) : void
    {

        $this->stopIfNecessary($this->options, $this->lastRestart, $this->job);

        if ($this->stopped) {
            $context->interruptExecution();
        }

    }


    public function onMessageReceived(MessageReceived $context): void
    {
        $this->job = $this->queue->convertMessageToJob(
            $context->getMessage(),
            $context->getConsumer()
        );

        if ($this->supportsAsyncSignals()) {
            $this->registerTimeoutHandler($this->job, $this->options);
        }
    }

    public function onPostMessageReceived(PostMessageReceived $context): void
    {
        $this->stopIfNecessary($this->options, $this->lastRestart, $this->job);

        if ($this->stopped) {
            $context->interruptExecution();
        }
    }

    public function stop($status = 0)
    {
        if ($this->interop) {
            $this->stopped = true;

            return;
        }

        parent::stop($status);
    }

    protected function pauseWorker(WorkerOptions $options, $lastRestart)
    {
        $this->sleep($options->sleep > 0 ? $options->sleep : 1);
        $this->stopIfNecessary($options, $lastRestart);
    }

}
