package com.eventstream.benchmark;

import com.eventstream.client.Producer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Single producer thread: sends fixed-size messages to the target topic at
 * maximum rate until the stop flag is set.
 *
 * One TCP connection per task.  A single-threaded, synchronous send loop is
 * intentional — it maximises per-connection throughput and ensures latency
 * measurements are clean (no pipelining overlap between requests).
 */
public final class ProducerTask implements Runnable {

    private final BenchmarkConfig  config;
    private final BenchmarkMetrics metrics;
    private final int              producerId;
    private volatile boolean       running = true;

    public ProducerTask(BenchmarkConfig config, BenchmarkMetrics metrics, int producerId) {
        this.config     = config;
        this.metrics    = metrics;
        this.producerId = producerId;
    }

    @Override
    public void run() {
        byte[] payload = new byte[config.msgSize];
        // Fill payload with a recognisable pattern to detect corruption.
        Arrays.fill(payload, (byte) ('A' + producerId % 26));

        try (Producer producer = new Producer(config.host, config.port)) {
            while (running) {
                long t0 = System.nanoTime();
                try {
                    producer.send(config.topic, payload);
                    metrics.recordSend(payload.length, System.nanoTime() - t0);
                } catch (IOException e) {
                    metrics.recordSendError();
                }
            }
        } catch (IOException e) {
            // Connection-level failure (broker down, etc.) — record and stop.
            metrics.recordSendError();
        }
    }

    public void stop() { running = false; }
}
