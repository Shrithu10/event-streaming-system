package com.eventstream.broker.topic;

import com.eventstream.broker.storage.LogSegment;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Owns all topics and their partitions.
 *
 * Phase 2 changes:
 *   - Topics are now represented by Topic (holding a Partition[] array) instead
 *     of a nested ConcurrentHashMap.  Array indexing is O(1) with no hash
 *     overhead on the hot produce/fetch path.
 *   - createTopic() accepts a numPartitions argument.
 *   - route() delegates to Topic.route() for key-hash or round-robin selection.
 *
 * Thread safety: ConcurrentHashMap.putIfAbsent guarantees that only one thread
 * initialises a given topic.  All partitions are fully built before the topic
 * is made visible, so getPartition() and route() never observe a partial state.
 */
public final class TopicManager implements Closeable {

    private static final Logger log = Logger.getLogger(TopicManager.class.getName());

    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private final Path logRoot;

    public TopicManager(Path logRoot) throws IOException {
        this.logRoot = logRoot;
        Files.createDirectories(logRoot);
        log.info("TopicManager started, log root: " + logRoot);
    }

    /**
     * Creates a topic with the given number of partitions.
     * @return true if created, false if already existed
     */
    public boolean createTopic(String name, int numPartitions) throws IOException {
        if (numPartitions < 1) throw new IllegalArgumentException("numPartitions must be >= 1");

        // Build all partitions before calling putIfAbsent.
        // If we lose the race, we close them and return false — disk dirs are idempotent.
        Partition[] partitions = new Partition[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            Path segDir = logRoot.resolve(name).resolve("partition-" + i);
            Files.createDirectories(segDir);
            LogSegment seg = new LogSegment(segDir.resolve("00000000.log"));
            partitions[i] = new Partition(name, i, seg);
        }

        Topic topic    = new Topic(name, partitions);
        Topic existing = topics.putIfAbsent(name, topic);
        if (existing != null) {
            for (Partition p : partitions) {
                try { p.close(); } catch (IOException ignored) {}
            }
            return false;
        }

        log.info("Topic created: " + name + " (partitions=" + numPartitions + ")");
        return true;
    }

    /**
     * Routes a produce request to a partition using the provided key.
     * Returns null if the topic does not exist.
     */
    public Partition route(String topicName, byte[] key) {
        Topic topic = topics.get(topicName);
        if (topic == null) return null;
        return topic.route(key);
    }

    /**
     * Returns a specific partition by id. Returns null if the topic or
     * partition id does not exist.
     */
    public Partition getPartition(String topicName, int partitionId) {
        Topic topic = topics.get(topicName);
        if (topic == null) return null;
        return topic.get(partitionId);
    }

    public int numPartitions(String topicName) {
        Topic topic = topics.get(topicName);
        return topic == null ? 0 : topic.numPartitions();
    }

    @Override
    public void close() {
        for (Topic topic : topics.values()) {
            for (Partition p : topic.partitions) {
                try { p.close(); } catch (IOException ignored) {}
            }
        }
    }
}
