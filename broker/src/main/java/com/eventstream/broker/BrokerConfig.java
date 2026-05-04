package com.eventstream.broker;

public final class BrokerConfig {

    public final int    port;
    public final String logDirectory;
    public final int    workerThreads;
    public final int    requestQueueCapacity;
    public final int    defaultPartitions;

    public BrokerConfig(int port, String logDirectory, int workerThreads,
                        int requestQueueCapacity, int defaultPartitions) {
        this.port                 = port;
        this.logDirectory         = logDirectory;
        this.workerThreads        = workerThreads;
        this.requestQueueCapacity = requestQueueCapacity;
        this.defaultPartitions    = defaultPartitions;
    }

    public static BrokerConfig defaults() {
        String home = System.getProperty("user.home");
        int    cpus = Runtime.getRuntime().availableProcessors();
        return new BrokerConfig(9092, home + "/eventstream-logs", cpus, 10_000, 1);
    }

    public static BrokerConfig fromArgs(String[] args) {
        int    port         = 9092;
        String logDir       = System.getProperty("user.home") + "/eventstream-logs";
        int    workers      = Runtime.getRuntime().availableProcessors();
        int    queueCap     = 10_000;
        int    defaultParts = 1;

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--port"       -> port         = Integer.parseInt(args[i + 1]);
                case "--logdir"     -> logDir       = args[i + 1];
                case "--workers"    -> workers      = Integer.parseInt(args[i + 1]);
                case "--queue-cap"  -> queueCap     = Integer.parseInt(args[i + 1]);
                case "--partitions" -> defaultParts = Integer.parseInt(args[i + 1]);
            }
        }
        return new BrokerConfig(port, logDir, workers, queueCap, defaultParts);
    }
}
