package com.pubsub.broker;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/** Batches elements from a BlockingQueue. */
class BlockingQueueBatcher {
    static RelativeTimeProvider timeProvider = new SystemRelativeTimeProvider();

    // Prevent instantiation.
    private BlockingQueueBatcher() {}

    public static <T> int take(BlockingQueue<T> queue,
                               Collection<? super T> batch, int maxBatchSize, long maxLatency,
                               TimeUnit maxLatencyUnit) throws InterruptedException {
        long maxLatencyNanos = maxLatencyUnit.toNanos(maxLatency);

        int curBatchSize = 0;
        long stopBatchTimeNanos = -1;

        // The loop flow is 1) block, 2) drain queue, 3) possibly consume batch.
        while (true) {
            boolean timeout = false;
            if (stopBatchTimeNanos == -1) {
                // Start of new batch. Block for the first item of this batch.
                batch.add(queue.take());
                curBatchSize++;
                stopBatchTimeNanos = timeProvider.relativeTime(TimeUnit.NANOSECONDS)
                        + maxLatencyNanos;
            } else {
                // Continue existing batch. Block until an item is in the queue or the
                // batch timeout expires.
                T element = queue.poll(
                        stopBatchTimeNanos
                                - timeProvider.relativeTime(TimeUnit.NANOSECONDS),
                        TimeUnit.NANOSECONDS);
                if (element == null) {
                    // Timeout occurred.
                    break;
                }
                batch.add(element);
                curBatchSize++;
            }
            curBatchSize += queue.drainTo(batch, maxBatchSize - curBatchSize);

            if (curBatchSize >= maxBatchSize) {
                // End current batch.
                break;
            }
        }

        return curBatchSize;
    }
}
