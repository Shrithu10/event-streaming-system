package com.eventstream.broker;

import com.eventstream.broker.topic.TopicManager;

import java.nio.file.Path;
import java.util.logging.Logger;

public final class BrokerMain {

    private static final Logger log = Logger.getLogger(BrokerMain.class.getName());

    public static void main(String[] args) throws Exception {
        BrokerConfig config = BrokerConfig.fromArgs(args);

        log.info("Starting broker | port=" + config.port + " | logDir=" + config.logDirectory);

        TopicManager topicManager = new TopicManager(Path.of(config.logDirectory));
        BrokerServer server       = new BrokerServer(config, topicManager);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown requested");
            server.stop();
            topicManager.close();
        }, "shutdown-hook"));

        server.start();
    }
}
