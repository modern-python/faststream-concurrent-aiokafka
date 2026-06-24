import asyncio
import bisect
import dataclasses
import operator
import typing

from faststream.kafka import TopicPartition


_OFFSET_KEY: typing.Final = operator.attrgetter("offset")


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class KafkaCommitTask:
    asyncio_task: asyncio.Task[typing.Any]
    topic_partition: TopicPartition
    offset: int
    consumer: typing.Any


def insert_sorted(partition_pending: list[KafkaCommitTask], new_ct: KafkaCommitTask) -> None:
    # Common case: tasks arrive from the broker in offset order, so append is correct and
    # the list stays sorted. Out-of-order arrivals only happen when _call_committer
    # re-queues a batch on transient KafkaError; bisect handles the rare case in O(log N).
    if not partition_pending or partition_pending[-1].offset <= new_ct.offset:
        partition_pending.append(new_ct)
    else:
        bisect.insort(partition_pending, new_ct, key=_OFFSET_KEY)


def extract_ready_prefixes(
    pending: dict[TopicPartition, list[KafkaCommitTask]],
) -> tuple[dict[TopicPartition, list[KafkaCommitTask]], int]:
    # Pending lists are maintained in offset order by insert_sorted. Per partition, find
    # the first not-done task; tasks before it form the contiguous-done prefix and become
    # "ready". A cancelled task is treated as a hard boundary: cancelled + everything after
    # is dropped from pending and added to ready (so task_done() balances
    # messages_queue.join), while map_offsets_per_partition stops the offset advance at
    # the cancelled task so the uncommitted offsets get redelivered on restart
    # (at-least-once). Returns (ready, count) so the caller can update its cached
    # pending_count without re-summing list lengths.
    ready: dict[TopicPartition, list[KafkaCommitTask]] = {}
    ready_count = 0
    empty_partitions: list[TopicPartition] = []
    for partition, partition_pending in pending.items():
        prefix_end = 0
        for index, task in enumerate(partition_pending):
            if task.asyncio_task.cancelled():
                prefix_end = len(partition_pending)
                break
            if not task.asyncio_task.done():
                prefix_end = index
                break
            prefix_end = index + 1

        if prefix_end > 0:
            ready[partition] = partition_pending[:prefix_end]
            ready_count += prefix_end
            del partition_pending[:prefix_end]
        if not partition_pending:
            empty_partitions.append(partition)

    for k in empty_partitions:
        del pending[k]
    return ready, ready_count


def map_offsets_per_partition(
    consumer_id: int,
    consumer_tasks: list[KafkaCommitTask],
    watermarks: dict[tuple[int, TopicPartition], int],
) -> dict[TopicPartition, int]:
    # `watermarks` is mutated: any cancelled task seen here records (or lowers) the
    # (consumer, partition) watermark. Subsequent batches for the same consumer will see
    # it and skip advancing past it. Other consumers (different group, same partition)
    # have their own keys and are unaffected. Caller (the committer) owns the dict.
    by_partition: dict[TopicPartition, list[KafkaCommitTask]] = {}
    for task in consumer_tasks:
        by_partition.setdefault(task.topic_partition, []).append(task)

    partitions_to_offsets: dict[TopicPartition, int] = {}
    for partition, tasks in by_partition.items():
        wm_key: tuple[int, TopicPartition] = (consumer_id, partition)
        max_offset: int | None = None
        for task in sorted(tasks, key=_OFFSET_KEY):
            if task.asyncio_task.cancelled():
                # Earliest cancelled wins: a later batch may not see the earlier
                # cancellation, so without min() we could forget it and accidentally
                # advance past the boundary.
                existing = watermarks.get(wm_key)
                if existing is None or task.offset < existing:
                    watermarks[wm_key] = task.offset
                break
            max_offset = task.offset
        if max_offset is None:
            continue
        wm = watermarks.get(wm_key)
        if wm is not None and (max_offset + 1) > wm:
            # Advancing would jump past the cancelled boundary — skip this partition
            # until the watermark is cleared on rebalance.
            continue
        # Kafka commits the *next* offset to fetch, so committed = processed_max + 1
        partitions_to_offsets[partition] = max_offset + 1
    return partitions_to_offsets


@dataclasses.dataclass(frozen=True, slots=True)
class ReadyCommit:
    # consumer typed Any to match KafkaCommitTask.consumer (avoids importing aiokafka at runtime)
    consumer: typing.Any
    offsets: dict[TopicPartition, int]
    tasks: list[KafkaCommitTask]


class PendingCommits:
    """Owns per-partition pending commit tasks, pending count, and cancellation watermarks.

    Synchronous and single-owner: the committer's streaming loop is the sole
    mutator, so no locking is needed. Reads asyncio task state (done/cancelled)
    but never awaits and never performs I/O.
    """

    def __init__(self) -> None:
        self._pending: dict[TopicPartition, list[KafkaCommitTask]] = {}
        self._count: int = 0
        self._watermarks: dict[tuple[int, TopicPartition], int] = {}

    def __len__(self) -> int:
        return self._count

    def absorb(self, ct: KafkaCommitTask) -> None:
        insert_sorted(self._pending.setdefault(ct.topic_partition, []), ct)
        self._count += 1

    def take_ready(self) -> list[ReadyCommit]:
        # Extract each partition's contiguous-done prefix (cancelled = hard
        # boundary), then group by consumer and apply the watermark floor.
        # Atomic and synchronous: pending + watermark mutation both happen here,
        # before any I/O the committer performs on the returned offsets.
        ready, ready_count = extract_ready_prefixes(self._pending)
        self._count -= ready_count
        flat: list[KafkaCommitTask] = [t for tasks in ready.values() for t in tasks]
        if not flat:
            return []
        by_consumer: dict[int, list[KafkaCommitTask]] = {}
        for task in flat:
            by_consumer.setdefault(id(task.consumer), []).append(task)
        result: list[ReadyCommit] = []
        for consumer_id, tasks in by_consumer.items():
            offsets = map_offsets_per_partition(consumer_id, tasks, self._watermarks)
            result.append(ReadyCommit(consumer=tasks[0].consumer, offsets=offsets, tasks=tasks))
        return result

    def clear_watermarks(self, partitions: typing.Iterable[TopicPartition] | None = None) -> None:
        # Drops every consumer's floor for the given partitions, not just one owner's. This
        # is safe because cancellation watermarks are only ever written during handler.stop()
        # — the sole path that cancels user tasks — when every consumer sharing this
        # PendingCommits is torn down together. There is therefore no live consumer whose
        # floor could be wrongly cleared here and then advance past its cancelled offset. If a
        # non-shutdown task-cancellation path is ever added, scope this to the revoked
        # consumer's id instead (see test_clear_cancellation_watermarks_delegates_to_pending).
        if partitions is None:
            self._watermarks.clear()
            return
        target: typing.Final = set(partitions)
        for key in [k for k in self._watermarks if k[1] in target]:
            del self._watermarks[key]
