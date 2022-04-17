package com.github.laz2.batcherizer.key;

import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Builder
@AllArgsConstructor
public class ConcurrentHashMapKeyBatcherizer<K, E> implements KeyBatcherizer<K, E> {

    private final int maxBatchSize;
    private final long maxBatchMs;
    private final ScheduledExecutorService scheduler;
    private final Consumer<Batch<K, E>> handler;

    private final ConcurrentHashMap<K, Batch<K, E>> batches = new ConcurrentHashMap<>();

    @Override
    public void add(K key, E data) {
        while (true) {
            var batch = batches.computeIfAbsent(key, Batch::new);
            synchronized (batch) {
                if (batch.getExpirationFut().isDone()) {
                    // Do not add to this batch because it has been removed by another thread
                    continue;
                }

                var values = batch.getValues();
                values.add(data);
                var size = values.size();
                if (size == 1) {
                    var expirationFut = scheduler.schedule(() -> {
                        removeBatch(batch);
                    }, maxBatchMs, TimeUnit.MILLISECONDS);
                    batch.setExpirationFut(expirationFut);
                } else if (size >= maxBatchSize) {
                    removeBatch(batch);
                }
            }

            break;
        }
    }

    private void removeBatch(Batch<K, E> batch) {
        var removed = batches.remove(batch.getKey(), batch);
        if (!removed) {
            // It has been removed by another thread
            return;
        }

        synchronized (batch) {
            batch.getExpirationFut().cancel(true);
            batch.setFinishBatchMs(System.currentTimeMillis());
            scheduler.execute(() -> handler.accept(batch));
        }
    }
}
