package com.eventstream.benchmark;

import com.eventstream.client.Producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Standalone benchmark harness for the Event Streaming System.
 *
 * Usage:
 *   java -jar benchmark.jar [options]
 *
 * Options (all optional, defaults shown):
 *   --host       localhost      broker host
 *   --port       9092           broker port
 *   --topic      bench-topic    topic name (created automatically)
 *   --partitions 4              number of partitions to create
 *   --producers  4              concurrent producer threads
 *   --consumers  4              concurrent consumer threads (one per partition, up to numPartitions)
 *   --msg-size   1024           message payload size in bytes
 *   --duration   30             benchmark duration in seconds
 *
 * Output format (printed to stdout at the end of the run):
 *
 *   === Benchmark Results ===
 *   Duration:    30s
 *   Producers:   4  |  Consumers: 4  |  Partitions: 4  |  Msg size: 1024 B
 *
 *   PRODUCE
 *     Sent:      1,234,567 messages  |  1,205 MB
 *     Rate:         41,152 msg/s     |    41.2 MB/s
 *     Errors:            0
 *     Latency (RTT):  p50=312 μs  p95=640 μs  p99=1024 μs
 *
 *   CONSUME
 *     Received:    987,654 messages  |   964 MB
 *     Rate:         32,921 msg/s     |    32.2 MB/s
 *     Errors:            0
 */
public final class BenchmarkHarness {

    public static void main(String[] args) throws Exception {
        BenchmarkConfig  config  = BenchmarkConfig.fromArgs(args);
        BenchmarkMetrics metrics = new BenchmarkMetrics();

        System.out.println("Starting benchmark: " + config);
        System.out.println();

        // ---- Setup: create topic ----
        try (Producer setup = new Producer(config.host, config.port)) {
            setup.createTopic(config.topic, config.numPartitions);
        } catch (IOException e) {
            System.err.println("Failed to create topic: " + e.getMessage());
            System.exit(1);
        }
        System.out.println("Topic '" + config.topic + "' ready (" + config.numPartitions + " partitions)");

        // ---- Start consumer threads (one per partition, capped at numConsumers) ----
        int numConsumerThreads = Math.min(config.numConsumers, config.numPartitions);
        List<ConsumerTask>  consumerTasks   = new ArrayList<>(numConsumerThreads);
        ExecutorService     consumerPool    = Executors.newFixedThreadPool(numConsumerThreads);
        for (int p = 0; p < numConsumerThreads; p++) {
            ConsumerTask task = new ConsumerTask(config, metrics, p);
            consumerTasks.add(task);
            consumerPool.submit(task);
        }

        // ---- Start producer threads ----
        List<ProducerTask>  producerTasks   = new ArrayList<>(config.numProducers);
        ExecutorService     producerPool    = Executors.newFixedThreadPool(config.numProducers);
        for (int i = 0; i < config.numProducers; i++) {
            ProducerTask task = new ProducerTask(config, metrics, i);
            producerTasks.add(task);
            producerPool.submit(task);
        }

        System.out.println("Running for " + config.durationSecs + "s …");
        long startMs = System.currentTimeMillis();

        // ---- Run ----
        Thread.sleep(config.durationSecs * 1000L);

        // ---- Shutdown ----
        long elapsed = System.currentTimeMillis() - startMs;

        for (ProducerTask t : producerTasks) t.stop();
        producerPool.shutdown();
        producerPool.awaitTermination(5, TimeUnit.SECONDS);

        for (ConsumerTask t : consumerTasks) t.stop();
        consumerPool.shutdown();
        consumerPool.awaitTermination(5, TimeUnit.SECONDS);

        // ---- Print results ----
        double secs     = elapsed / 1000.0;
        long sentCount  = metrics.sentCount();
        long sentBytes  = metrics.sentBytes();
        long recvCount  = metrics.recvCount();
        long recvBytes  = metrics.recvBytes();

        long produceMsgPerSec  = (long)(sentCount / secs);
        double produceMBPerSec = sentBytes / secs / (1024.0 * 1024.0);
        long consumeMsgPerSec  = (long)(recvCount / secs);
        double consumeMBPerSec = recvBytes / secs / (1024.0 * 1024.0);

        System.out.println();
        System.out.println("=== Benchmark Results ===");
        System.out.printf("Duration:    %.1fs%n", secs);
        System.out.printf("Producers:   %d  |  Consumers: %d  |  Partitions: %d  |  Msg size: %d B%n%n",
                config.numProducers, numConsumerThreads, config.numPartitions, config.msgSize);

        System.out.println("PRODUCE");
        System.out.printf("  Sent:      %,d messages  |  %.0f MB%n", sentCount, sentBytes / (1024.0 * 1024.0));
        System.out.printf("  Rate:      %,d msg/s     |  %.1f MB/s%n", produceMsgPerSec, produceMBPerSec);
        System.out.printf("  Errors:    %,d%n", metrics.sendErrors());
        System.out.printf("  Latency:   p50=%s  p95=%s  p99=%s%n%n",
                BenchmarkMetrics.fmtNs(metrics.sendP50Ns()),
                BenchmarkMetrics.fmtNs(metrics.sendP95Ns()),
                BenchmarkMetrics.fmtNs(metrics.sendP99Ns()));

        System.out.println("CONSUME");
        System.out.printf("  Received:  %,d messages  |  %.0f MB%n", recvCount, recvBytes / (1024.0 * 1024.0));
        System.out.printf("  Rate:      %,d msg/s     |  %.1f MB/s%n", consumeMsgPerSec, consumeMBPerSec);
        System.out.printf("  Errors:    %,d%n", metrics.recvErrors());
    }
}
