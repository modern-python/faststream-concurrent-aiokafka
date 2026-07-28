import asyncio
import functools
import inspect
import logging
import typing

from faststream.exceptions import IgnoredException
from faststream.kafka import ConsumerRecord, TopicPartition
from faststream.kafka.message import KafkaAckableMessage

from faststream_concurrent_aiokafka import batch_committer, consts
from faststream_concurrent_aiokafka._signals import classify_signal
from faststream_concurrent_aiokafka.batch_committer import KafkaBatchCommitter
from faststream_concurrent_aiokafka.rebalance import ConsumerRebalanceListener


logger = logging.getLogger(__name__)


async def _absorb_control_signal(coroutine: typing.Awaitable[typing.Any]) -> typing.Any:  # noqa: ANN401
    """Run the user coroutine, absorbing any FastStream control signal raised inside it.

    A middleware registered *after* `KafkaConcurrentProcessingMiddleware` runs inside this
    coroutine, so a signal it raises would otherwise end the asyncio task. That costs three
    things no log level can fix. The task keeps the exception, its traceback, and every frame
    the traceback references - including the message body - alive until the committer commits
    the offset. Asyncio task-factory wrappers such as `sentry_sdk`'s `AsyncioIntegration` see
    it inside the task and report it as an unhandled error. And `StopApplication` subclasses
    `SystemExit`, which asyncio's `Task.__step` re-raises into the event loop, killing the
    application outright with in-flight offsets uncommitted.

    Absorbing is offset-neutral: the task completes normally, and the committer commits a task
    that is done and not cancelled either way. `_signals.classify_signal` decides the log level
    and says whether absorbing actually honours the caller's request.
    """
    try:
        return await coroutine
    except IgnoredException as exc:
        policy: typing.Final = classify_signal(exc)
        logger.log(policy.level, "Kafka middleware. %s (%s)", policy.reason, type(exc).__name__)
        return None


class KafkaConcurrentHandler:
    def __init__(
        self,
        committer: KafkaBatchCommitter,
        concurrency_limit: int = consts.DEFAULT_CONCURRENCY_LIMIT,
    ) -> None:
        if concurrency_limit < 1:
            msg = f"concurrency_limit must be >= 1, got {concurrency_limit}"
            raise ValueError(msg)

        self._limiter = asyncio.Semaphore(concurrency_limit)
        # Tracked only so stop() can cancel them. The committer is the source of truth for
        # offset progress; this set just lets us reach in-flight tasks at shutdown.
        self._tracked_tasks: set[asyncio.Task[typing.Any]] = set()
        self._is_running: bool = False
        self._committer: KafkaBatchCommitter = committer

    def _finish_task(self, coroutine: typing.Awaitable[typing.Any], task: asyncio.Task[typing.Any]) -> None:
        self._limiter.release()
        self._tracked_tasks.discard(task)
        if task.cancelled():
            # stop() can cancel the _absorb_control_signal shield before its first step, so the
            # shield never awaited `coroutine`. Close it here: an un-awaited coroutine emits a
            # RuntimeWarning through sys.unraisablehook, which lands in the app's logs and
            # error reporter. Before the shield existed the Task owned `coroutine` directly
            # and closed it on cancellation; this preserves that.
            if inspect.iscoroutine(coroutine) and inspect.getcoroutinestate(coroutine) == inspect.CORO_CREATED:
                coroutine.close()
            return
        exc: typing.Final[BaseException | None] = task.exception()
        if exc:
            logger.error("Kafka middleware. Task has failed with the exception", exc_info=exc)

    async def handle_task(
        self,
        coroutine: typing.Awaitable[typing.Any],
        record: ConsumerRecord,
        kafka_message: KafkaAckableMessage,
    ) -> None:
        await self._limiter.acquire()
        task: typing.Final = asyncio.ensure_future(_absorb_control_signal(coroutine))
        self._tracked_tasks.add(task)
        task.add_done_callback(functools.partial(self._finish_task, coroutine))
        try:
            await self._committer.send_task(
                batch_committer.KafkaCommitTask(
                    asyncio_task=task,
                    offset=record.offset,
                    consumer=kafka_message.consumer,
                    topic_partition=TopicPartition(topic=record.topic, partition=record.partition),
                )
            )
        except batch_committer.CommitterIsDeadError:
            logger.exception("Kafka middleware. Committer is dead")
            await self.stop()
            raise

    async def start(self) -> None:
        if self._is_running:
            return

        logger.info("Kafka middleware. Start middleware handler")
        self._is_running = True

        self._committer.spawn()
        logger.info("Kafka middleware is ready to process messages.")

    async def stop(self) -> None:
        if not self._is_running:
            return
        logger.info("Kafka middleware. Shutting down middleware handler")
        self._is_running = False

        # Cancel in-flight user tasks. The committer treats cancelled tasks as a hard
        # offset boundary (_pending_state.extract_ready_prefixes / map_offsets_per_partition):
        # cancelled-and-after offsets stay uncommitted and get redelivered on restart.
        # Task.cancel() is a no-op on already-done tasks.
        for task in list(self._tracked_tasks):
            task.cancel()

        await self._committer.close()

        logger.info("Kafka middleware. Complete shutting down middleware handler")

    def create_rebalance_listener(
        self, flush_timeout_sec: float = consts.DEFAULT_REBALANCE_FLUSH_TIMEOUT_SEC
    ) -> ConsumerRebalanceListener:
        """Return a ConsumerRebalanceListener that flushes pending commits on partition revocation.

        ``flush_timeout_sec`` bounds how long the revoke callback waits for in-flight
        handlers to finish before returning (keeps a slow handler from stalling the
        rebalance past max.poll.interval.ms). Pass the returned listener to
        ``@broker.subscriber(..., listener=listener)``.
        """
        return ConsumerRebalanceListener(self._committer, flush_timeout_sec)

    @property
    def is_healthy(self) -> bool:
        return self._is_running and self._committer.is_healthy

    @property
    def is_running(self) -> bool:
        return self._is_running
