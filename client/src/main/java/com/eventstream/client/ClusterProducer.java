package com.eventstream.client;

import com.eventstream.common.protocol.ClusterConfig;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Metadata-aware producer for cluster mode.
 *
 * Routes PRODUCE requests to the correct leader broker for each partition.
 * On {@link NotLeaderException}, refreshes metadata and retries once.
 *
 * Each distinct leader brokerId gets its own persistent {@link Producer}
 * connection so connection setup overhead is paid only once per broker.
 *
 * Usage:
 *   try (ClusterProducer cp = new ClusterProducer("localhost", 9092)) {
 *       cp.createTopic("orders", 4);
 *       cp.send("orders", "user-1", "event payload");
 *   }
 */
public final class ClusterProducer implements Closeable {

    private static final int MAX_RETRIES = 2;

    private final MetadataClient         metadataClient;
    private final Map<Integer, Producer> connections = new HashMap<>(); // brokerId → Producer

    public ClusterProducer(String bootstrapHost, int bootstrapPort) {
        this.metadataClient = new MetadataClient(bootstrapHost, bootstrapPort);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    public void createTopic(String topic, int numPartitions) throws IOException {
        // CREATE_TOPIC is idempotent on every broker; send to any available broker.
        producerFor(bootstrapBrokerId()).createTopic(topic, numPartitions);
    }

    public Producer.SendResult send(String topic, String key, String message) throws IOException {
        return send(topic,
                key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                message.getBytes(StandardCharsets.UTF_8));
    }

    public Producer.SendResult send(String topic, byte[] key, byte[] payload) throws IOException {
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            ClusterConfig config   = metadataClient.metadata();
            int           leaderId = resolveLeader(config, topic);
            try {
                return producerFor(leaderId).send(topic, key, payload);
            } catch (NotLeaderException e) {
                // Stale metadata — evict the stale connection and refresh.
                closeConnectionFor(leaderId);
                metadataClient.refresh();
            }
        }
        throw new IOException("Failed to produce to topic '" + topic + "' after "
                + MAX_RETRIES + " attempts — no reachable leader");
    }

    @Override
    public synchronized void close() {
        for (Producer p : connections.values()) {
            try { p.close(); } catch (IOException ignored) {}
        }
        connections.clear();
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    /**
     * Returns the leaderId for any partition of the given topic.
     * When multiple partitions exist, uses the first assignment found.
     * The broker-side routing (Murmur2 / round-robin) picks the actual partition;
     * all partitions of the same topic on different leaders would require per-key
     * routing here (Phase 5+ improvement).
     */
    private int resolveLeader(ClusterConfig config, String topic) {
        return config.assignments().stream()
                .filter(a -> a.topic().equals(topic))
                .mapToInt(ClusterConfig.PartitionAssignment::leaderId)
                .findFirst()
                .orElseGet(this::bootstrapBrokerId);
    }

    private synchronized Producer producerFor(int brokerId) throws IOException {
        if (!connections.containsKey(brokerId)) {
            ClusterConfig config   = metadataClient.metadata();
            ClusterConfig.BrokerInfo info = config.broker(brokerId)
                    .orElseThrow(() -> new IOException("Unknown broker id: " + brokerId));
            connections.put(brokerId, new Producer(info.host(), info.port()));
        }
        return connections.get(brokerId);
    }

    private synchronized void closeConnectionFor(int brokerId) {
        Producer p = connections.remove(brokerId);
        if (p != null) try { p.close(); } catch (IOException ignored) {}
    }

    private int bootstrapBrokerId() {
        try {
            ClusterConfig config = metadataClient.metadata();
            return config.brokers().isEmpty() ? 1 : config.brokers().get(0).brokerId();
        } catch (IOException e) {
            return 1;
        }
    }
}
