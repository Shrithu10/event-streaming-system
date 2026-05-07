package com.eventstream.broker;

import com.eventstream.broker.cluster.ClusterMetadata;
import com.eventstream.broker.cluster.ReplicationManager;
import com.eventstream.broker.group.ConsumerGroupManager;
import com.eventstream.broker.metrics.BrokerMetrics;
import com.eventstream.broker.metrics.MetricsReporter;
import com.eventstream.broker.topic.TopicManager;
import com.eventstream.common.protocol.ClusterConfig;

import java.nio.file.Path;
import java.util.logging.Logger;

public final class BrokerMain {

    private static final Logger log = Logger.getLogger(BrokerMain.class.getName());

    public static void main(String[] args) throws Exception {
        BrokerConfig config = BrokerConfig.fromArgs(args);

        // Phase 4: load cluster config or fall back to single-node mode.
        ClusterConfig clusterConfig = config.clusterConfigPath != null
                ? ClusterConfig.fromFile(Path.of(config.clusterConfigPath))
                : ClusterConfig.singleNode(config.brokerId, "localhost", config.port);

        log.info("Starting broker | brokerId=" + config.brokerId
                + " | port=" + config.port
                + " | workers=" + config.workerThreads
                + " | logDir=" + config.logDirectory
                + " | sessionTimeout=" + config.sessionTimeoutMs + "ms"
                + " | clusterMode=" + !clusterConfig.isSingleNodeMode());

        TopicManager         topicManager        = new TopicManager(Path.of(config.logDirectory));
        ConsumerGroupManager groupManager        = new ConsumerGroupManager(topicManager,
                                                                            config.reaperIntervalMs);
        ClusterMetadata      clusterMetadata     = new ClusterMetadata(config.brokerId, clusterConfig);
        ReplicationManager   replicationManager  = new ReplicationManager(clusterMetadata, topicManager);
        replicationManager.start();

        MetricsReporter metricsReporter = new MetricsReporter(BrokerMetrics.get());
        metricsReporter.start();

        BrokerServer server = new BrokerServer(config, topicManager, groupManager, replicationManager);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown requested");
            server.stop();
            metricsReporter.stop();
            replicationManager.close();
            groupManager.close();
            topicManager.close();
        }, "shutdown-hook"));

        server.start();
    }
}
