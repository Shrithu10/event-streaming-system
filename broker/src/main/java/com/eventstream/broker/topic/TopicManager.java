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
 * createTopic() is idempotent: creating the same topic twice returns false
 * (already-exists) but leaves the system consistent.
 *
 * Thread safety: ConcurrentHashMap.putIfAbsent guarantees that only one
 * thread initialises a given topic, and the partition map is fully populated
 * before it becomes visible to any reader.
 */
public final class TopicManager implements Closeable {

    private static final Logger log = Logger.getLogger(TopicManager.class.getName());

    // topicName -> (partitionId -> Partition)
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Partition>> topics =
            new ConcurrentHashMap<>();

    private final Path logRoot;

    public TopicManager(Path logRoot) throws IOException {
        this.logRoot = logRoot;
        Files.createDirectories(logRoot);
        log.info("TopicManager started, log root: " + logRoot);
    }

    /**
     * Creates topic with a single partition (partition 0).
     * @return true if created, false if already existed
     */
    public boolean createTopic(String name) throws IOException {
        // Build the partition before making the topic visible so that any
        // concurrent getPartition() call never observes a topic without p-0.
        Path segDir = logRoot.resolve(name).resolve("partition-0");
        Files.createDirectories(segDir);
        LogSegment segment = new LogSegment(segDir.resolve("00000000.log"));
        Partition p = new Partition(name, 0, segment);

        ConcurrentHashMap<Integer, Partition> partitions = new ConcurrentHashMap<>();
        partitions.put(0, p);

        ConcurrentHashMap<Integer, Partition> existing = topics.putIfAbsent(name, partitions);
        if (existing != null) {
            // Lost the race — discard the partition we just built.
            p.close();
            return false;
        }

        log.info("Topic created: " + name);
        return true;
    }

    /**
     * Returns the requested partition, or null if the topic / partition does
     * not exist.
     */
    public Partition getPartition(String topicName, int partitionId) {
        ConcurrentHashMap<Integer, Partition> parts = topics.get(topicName);
        if (parts == null) return null;
        return parts.get(partitionId);
    }

    @Override
    public void close() {
        for (var parts : topics.values()) {
            for (var partition : parts.values()) {
                try { partition.close(); } catch (IOException ignored) {}
            }
        }
    }
}
