package com.eventstream.broker;

import com.eventstream.broker.group.ConsumerGroupManager;
import com.eventstream.broker.topic.TopicManager;

import java.nio.file.Path;
import java.util.logging.Logger;

public final class BrokerMain {

    private static final Logger log = Logger.getLogger(BrokerMain.class.getName());

    public static void main(String[] args) throws Exception {
        BrokerConfig config = BrokerConfig.fromArgs(args);

        log.info("Starting broker | port=" + config.port
                + " | workers=" + config.workerThreads
                + " | logDir=" + config.logDirectory
                + " | sessionTimeout=" + config.sessionTimeoutMs + "ms");

        TopicManager         topicManager = new TopicManager(Path.of(config.logDirectory));
        ConsumerGroupManager groupManager = new ConsumerGroupManager(topicManager,
                                                                     config.reaperIntervalMs);
        BrokerServer         server       = new BrokerServer(config, topicManager, groupManager);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown requested");
            server.stop();
            groupManager.close();
            topicManager.close();
        }, "shutdown-hook"));

        server.start();
    }
}
