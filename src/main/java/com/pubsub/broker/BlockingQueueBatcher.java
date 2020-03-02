package com.pubsub.broker;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockingQueueBatcher {

    static RelativeTimeProvider timeProvider = new SystemRelativeTimeProvider();

    // Prevent instantiation.
    private BlockingQueueBatcher() {
    }

    public static <T> int take(BlockingQueue<T> queue,
                               Collection<? super T> batch,
                               int maxBatchSize,
                               long maxLatency) throws InterruptedException {

        int curBatchSize = 0;

        while (true) {
            T element = queue.poll(maxLatency, TimeUnit.MILLISECONDS);
            if (element == null) {
                break;
            }
            batch.add(element);
            curBatchSize++;
            curBatchSize += queue.drainTo(batch, maxBatchSize - curBatchSize);

            if (curBatchSize >= maxBatchSize) {
                break;
            }
        }

        return curBatchSize;
    }
}

