package com.github.laz2.batcherizer.key;

import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Builder
@AllArgsConstructor
public class SynchronizedKeyBatcherizer<K, E> implements KeyBatcherizer<K, E> {

    private final int maxBatchSize;
    private final long maxBatchMs;
    private final ScheduledExecutorService scheduler;
    private final Consumer<Batch<K, E>> handler;

    private final Map<K, Batch<K, E>> batches = new HashMap<>();

    public synchronized void add(K key, E data) {
        var batch = batches.computeIfAbsent(key, Batch::new);
        batch.getValues().add(data);

        var size = batch.getValues().size();
        if (size == 1) {
            var expirationFut = scheduler.schedule(() -> {
                removeBatch(batch.getKey());
            }, maxBatchMs, TimeUnit.MILLISECONDS);
            batch.setExpirationFut(expirationFut);
        } else if (size >= maxBatchSize) {
            removeBatch(batch.getKey());
        }
    }

    private synchronized void removeBatch(K key) {
        var batch = batches.remove(key);
        if (batch != null) {
            batch.getExpirationFut().cancel(true);
            batch.setFinishBatchMs(System.currentTimeMillis());
            scheduler.execute(() -> handler.accept(batch));
        }
    }
}
