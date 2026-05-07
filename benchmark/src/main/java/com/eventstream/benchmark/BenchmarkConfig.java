package com.eventstream.benchmark;

/**
 * Benchmark configuration parsed from command-line arguments.
 *
 * Defaults represent a realistic mid-load scenario: 4 concurrent producers,
 * 4 consumers, 4 partitions, 1 KiB messages, 30-second run.
 */
public final class BenchmarkConfig {

    public final String host;
    public final int    port;
    public final String topic;
    public final int    numPartitions;
    public final int    numProducers;
    public final int    numConsumers;
    public final int    msgSize;      // payload bytes per message
    public final int    durationSecs;

    public BenchmarkConfig(String host, int port, String topic,
                           int numPartitions, int numProducers, int numConsumers,
                           int msgSize, int durationSecs) {
        this.host          = host;
        this.port          = port;
        this.topic         = topic;
        this.numPartitions = numPartitions;
        this.numProducers  = numProducers;
        this.numConsumers  = numConsumers;
        this.msgSize       = msgSize;
        this.durationSecs  = durationSecs;
    }

    public static BenchmarkConfig defaults() {
        return new BenchmarkConfig("localhost", 9092, "bench-topic",
                4, 4, 4, 1024, 30);
    }

    public static BenchmarkConfig fromArgs(String[] args) {
        String host         = "localhost";
        int    port         = 9092;
        String topic        = "bench-topic";
        int    partitions   = 4;
        int    producers    = 4;
        int    consumers    = 4;
        int    msgSize      = 1024;
        int    durationSecs = 30;

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--host"       -> host         = args[i + 1];
                case "--port"       -> port         = Integer.parseInt(args[i + 1]);
                case "--topic"      -> topic        = args[i + 1];
                case "--partitions" -> partitions   = Integer.parseInt(args[i + 1]);
                case "--producers"  -> producers    = Integer.parseInt(args[i + 1]);
                case "--consumers"  -> consumers    = Integer.parseInt(args[i + 1]);
                case "--msg-size"   -> msgSize      = Integer.parseInt(args[i + 1]);
                case "--duration"   -> durationSecs = Integer.parseInt(args[i + 1]);
            }
        }
        return new BenchmarkConfig(host, port, topic, partitions, producers, consumers,
                msgSize, durationSecs);
    }

    @Override
    public String toString() {
        return String.format(
                "BenchmarkConfig{host=%s, port=%d, topic=%s, partitions=%d, " +
                "producers=%d, consumers=%d, msgSize=%d, duration=%ds}",
                host, port, topic, numPartitions, numProducers, numConsumers,
                msgSize, durationSecs);
    }
}
