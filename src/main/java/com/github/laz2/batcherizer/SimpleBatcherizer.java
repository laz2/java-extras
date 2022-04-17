package com.github.laz2.batcherizer;

import com.github.laz2.utils.ThreadUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

@Slf4j
public class SimpleBatcherizer<E> {

    private final BatchQueue<E> queue;
    private final int maxBatchSize;
    private final long maxBatchMs;
    private final Handler<E> handler;

    public SimpleBatcherizer(String name, int maxBatchSize, long maxBatchMs, Handler<E> handler) {
        this.queue = new BatchQueue<>();
        this.maxBatchSize = maxBatchSize;
        this.maxBatchMs = maxBatchMs;
        this.handler = Objects.requireNonNull(handler);

        var worker = new Thread(
            this::runImpl,
            ThreadUtils.getThreadName(name, "batcherizer")
        );
        worker.setDaemon(true);
        worker.start();
    }

    public void add(E element) {
        queue.add(element);
    }

    private void runImpl() {
        log.debug("Started");
        try {
            while (true) {
                try {
                    runBatchImpl();
                } catch (InterruptedException e) {
                    log.info("Thread was interrupted");
                    break;
                } catch (Throwable e) {
                    log.error("Exception while executing:", e);
                }
            }
        } finally {
            log.info("Terminated");
        }
    }

    private void runBatchImpl() throws InterruptedException {
        var batch = queue.take(maxBatchSize, maxBatchMs);
        handler.process(batch);
    }

    public interface Handler<E> {

        void process(Batch<E> batch);
    }

    @Data
    @Accessors(chain = true)
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Batch<E> {

        private List<E> values;
        private long startBatchMs;
        private long finishBatchMs;

        public long getBatchMs() {
            return finishBatchMs - startBatchMs;
        }

        public boolean isEmpty() {
            return values.isEmpty();
        }

        public int size() {
            return values.size();
        }
    }
}
