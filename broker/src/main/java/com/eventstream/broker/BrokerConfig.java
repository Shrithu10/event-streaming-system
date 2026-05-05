package com.eventstream.broker;

public final class BrokerConfig {

    public final int    port;
    public final String logDirectory;
    public final int    workerThreads;
    public final int    requestQueueCapacity;
    public final int    defaultPartitions;
    public final int    sessionTimeoutMs;
    public final int    reaperIntervalMs;
    // Phase 4
    public final int    brokerId;
    public final String clusterConfigPath; // null = single-node mode

    public BrokerConfig(int port, String logDirectory, int workerThreads,
                        int requestQueueCapacity, int defaultPartitions,
                        int sessionTimeoutMs, int reaperIntervalMs,
                        int brokerId, String clusterConfigPath) {
        this.port                 = port;
        this.logDirectory         = logDirectory;
        this.workerThreads        = workerThreads;
        this.requestQueueCapacity = requestQueueCapacity;
        this.defaultPartitions    = defaultPartitions;
        this.sessionTimeoutMs     = sessionTimeoutMs;
        this.reaperIntervalMs     = reaperIntervalMs;
        this.brokerId             = brokerId;
        this.clusterConfigPath    = clusterConfigPath;
    }

    public static BrokerConfig defaults() {
        String home = System.getProperty("user.home");
        int    cpus = Runtime.getRuntime().availableProcessors();
        return new BrokerConfig(9092, home + "/eventstream-logs",
                cpus, 10_000, 1, 30_000, 1_000, 1, null);
    }

    public static BrokerConfig fromArgs(String[] args) {
        int    port              = 9092;
        String logDir            = System.getProperty("user.home") + "/eventstream-logs";
        int    workers           = Runtime.getRuntime().availableProcessors();
        int    queueCap          = 10_000;
        int    defaultParts      = 1;
        int    sessionMs         = 30_000;
        int    reaperMs          = 1_000;
        int    brokerId          = 1;
        String clusterConfigPath = null;

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--port"            -> port             = Integer.parseInt(args[i + 1]);
                case "--logdir"          -> logDir           = args[i + 1];
                case "--workers"         -> workers          = Integer.parseInt(args[i + 1]);
                case "--queue-cap"       -> queueCap         = Integer.parseInt(args[i + 1]);
                case "--partitions"      -> defaultParts     = Integer.parseInt(args[i + 1]);
                case "--session-timeout" -> sessionMs        = Integer.parseInt(args[i + 1]);
                case "--reaper-interval" -> reaperMs         = Integer.parseInt(args[i + 1]);
                case "--broker-id"       -> brokerId         = Integer.parseInt(args[i + 1]);
                case "--cluster-config"  -> clusterConfigPath = args[i + 1];
            }
        }
        return new BrokerConfig(port, logDir, workers, queueCap, defaultParts,
                sessionMs, reaperMs, brokerId, clusterConfigPath);
    }
}
