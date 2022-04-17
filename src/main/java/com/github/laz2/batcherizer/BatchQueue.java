package com.github.laz2.batcherizer;

import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class BatchQueue<E> {

    private final BlockingQueue<E> queue = new LinkedBlockingDeque<>();

    public void add(E element) {
        queue.add(element);
    }

    public SimpleBatcherizer.Batch<E> take(int maxBatchSize, long maxBatchMs) throws InterruptedException {
        var element = queue.take();

        var maxBatchNs = TimeUnit.MILLISECONDS.toNanos(maxBatchMs);
        var startWaitMs = System.currentTimeMillis();
        var batch = new ArrayList<E>(maxBatchSize);
        batch.add(element);

        var endNs = System.nanoTime() + maxBatchNs;
        while (true) {
            var nowNs = System.nanoTime();
            if (batch.size() >= maxBatchSize || nowNs >= endNs) {
                break;
            }

            element = queue.poll(endNs - nowNs, TimeUnit.NANOSECONDS);
            if (element != null) {
                batch.add(element);
                if (batch.size() < maxBatchSize)
                    queue.drainTo(batch, maxBatchSize - batch.size());
            } else {
                break;
            }
        }

        return new SimpleBatcherizer.Batch<>(batch, startWaitMs, System.currentTimeMillis());
    }
}
