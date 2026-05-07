package com.eventstream.benchmark;

import com.eventstream.client.Consumer;
import com.eventstream.client.FetchResult;

import java.io.IOException;

/**
 * Single consumer thread: polls one assigned partition at maximum rate until
 * the stop flag is set.  Uses the plain (non-group) fetch path to avoid
 * consumer-group coordination overhead in the benchmark.
 *
 * Starts from offset 0 so it also counts messages produced before the
 * consumer started.  Under sustained load the consumer will lag behind the
 * producer; the total receive count therefore reflects the consumer's
 * throughput capacity, not the producer's.
 */
public final class ConsumerTask implements Runnable {

    private final BenchmarkConfig  config;
    private final BenchmarkMetrics metrics;
    private final int              partitionId;
    private volatile boolean       running = true;

    public ConsumerTask(BenchmarkConfig config, BenchmarkMetrics metrics, int partitionId) {
        this.config      = config;
        this.metrics     = metrics;
        this.partitionId = partitionId;
    }

    @Override
    public void run() {
        try (Consumer consumer = new Consumer(config.host, config.port)) {
            long offset = 0;
            while (running) {
                try {
                    FetchResult result = consumer.poll(config.topic, partitionId, offset);
                    if (result.isEmpty()) {
                        // Caught up — brief yield to avoid busy-spinning on the selector.
                        Thread.yield();
                        continue;
                    }
                    for (byte[] msg : result.messages) {
                        metrics.recordReceive(msg.length);
                    }
                    offset = result.nextOffset;
                } catch (IOException e) {
                    metrics.recordReceiveError();
                }
            }
        } catch (IOException e) {
            metrics.recordReceiveError();
        }
    }

    public void stop() { running = false; }
}
