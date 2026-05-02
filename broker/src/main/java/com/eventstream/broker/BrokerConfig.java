package com.eventstream.broker;

public final class BrokerConfig {

    public final int    port;
    public final String logDirectory;

    public BrokerConfig(int port, String logDirectory) {
        this.port         = port;
        this.logDirectory = logDirectory;
    }

    public static BrokerConfig defaults() {
        String home = System.getProperty("user.home");
        return new BrokerConfig(9092, home + "/eventstream-logs");
    }

    public static BrokerConfig fromArgs(String[] args) {
        int    port   = 9092;
        String logDir = System.getProperty("user.home") + "/eventstream-logs";

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--port"   -> port   = Integer.parseInt(args[i + 1]);
                case "--logdir" -> logDir = args[i + 1];
            }
        }
        return new BrokerConfig(port, logDir);
    }
}
