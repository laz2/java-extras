package com.github.laz2.batcherizer.key;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

public interface KeyBatcherizer<K, E> {

    void add(K key, E data);

    @Data
    @Accessors(chain = true)
    @NoArgsConstructor
    @AllArgsConstructor
    class Batch<K, E> {

        private K key;
        private List<E> values = new ArrayList<>();
        private long startBatchMs;
        private long finishBatchMs;
        private ScheduledFuture<?> expirationFut;

        public Batch(K key) {
            this.key = key;
            this.startBatchMs = System.currentTimeMillis();
        }

        public long getBatchMs() {
            return finishBatchMs - startBatchMs;
        }
    }
}
