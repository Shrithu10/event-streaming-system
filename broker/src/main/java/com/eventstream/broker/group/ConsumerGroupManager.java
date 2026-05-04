package com.eventstream.broker.group;

import com.eventstream.broker.topic.TopicManager;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Owns all consumer groups and drives the group lifecycle.
 *
 * Public methods are called from worker threads (via request handlers).
 * The heartbeat reaper runs on its own scheduled thread.
 *
 * Thread safety per operation:
 *
 *   joinGroup / leaveGroup — acquire group.rebalanceLock, mutate members,
 *     call rebalance(), release lock.  Short critical section: no I/O inside.
 *
 *   heartbeat — updates a volatile long on ConsumerMember; no lock needed.
 *     Returns REBALANCE_IN_PROGRESS if generationId doesn't match so the
 *     consumer knows to re-join.
 *
 *   commitOffset — single AtomicLongArray.set(); O(1), lock-free.
 *
 *   evictDeadConsumers — runs on the reaper thread every reaperIntervalMs.
 *     Acquires rebalanceLock only when at least one dead member is found.
 */
public final class ConsumerGroupManager implements Closeable {

    private static final Logger log = Logger.getLogger(ConsumerGroupManager.class.getName());

    private final ConcurrentHashMap<String, ConsumerGroup> groups = new ConcurrentHashMap<>();
    private final TopicManager          topicManager;
    private final ScheduledExecutorService reaper;

    public ConsumerGroupManager(TopicManager topicManager, int reaperIntervalMs) {
        this.topicManager = topicManager;
        this.reaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-reaper");
            t.setDaemon(true);
            return t;
        });
        reaper.scheduleAtFixedRate(this::evictDeadConsumers,
                reaperIntervalMs, reaperIntervalMs, TimeUnit.MILLISECONDS);
        log.info("ConsumerGroupManager started (reaperInterval=" + reaperIntervalMs + "ms)");
    }

    // -------------------------------------------------------------------------
    // Join
    // -------------------------------------------------------------------------

    /**
     * Registers a consumer in a group and triggers a rebalance.
     *
     * @return JoinResult with the new generationId and this consumer's assigned
     *         partitions, or null if the topic doesn't exist.
     */
    public JoinResult joinGroup(String groupId, String consumerId,
                                String topicName, int sessionTimeoutMs) {
        int numPartitions = topicManager.numPartitions(topicName);
        if (numPartitions == 0) return null; // topic not found

        ConsumerGroup group = groups.computeIfAbsent(groupId,
                id -> new ConsumerGroup(id, topicName, numPartitions));

        group.rebalanceLock.lock();
        try {
            // Re-join: update heartbeat and session timeout if already a member.
            group.members.put(consumerId, new ConsumerMember(consumerId, sessionTimeoutMs));
            group.rebalance();
            ConsumerMember m = group.members.get(consumerId);
            log.info("Consumer joined: group=" + groupId + " consumer=" + consumerId
                    + " genId=" + group.generationId
                    + " partitions=" + java.util.Arrays.toString(m.assignedPartitions));
            return new JoinResult(group.generationId, m.assignedPartitions);
        } finally {
            group.rebalanceLock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Leave
    // -------------------------------------------------------------------------

    public LeaveResult leaveGroup(String groupId, String consumerId) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null) return LeaveResult.UNKNOWN_GROUP;

        group.rebalanceLock.lock();
        try {
            if (group.members.remove(consumerId) == null) return LeaveResult.UNKNOWN_CONSUMER;
            group.rebalance();
            log.info("Consumer left: group=" + groupId + " consumer=" + consumerId
                    + " newGenId=" + group.generationId);
            return LeaveResult.OK;
        } finally {
            group.rebalanceLock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Heartbeat
    // -------------------------------------------------------------------------

    public HeartbeatResult heartbeat(String groupId, String consumerId, int generationId) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null) return HeartbeatResult.UNKNOWN_GROUP;

        ConsumerMember m = group.members.get(consumerId);
        if (m == null) return HeartbeatResult.UNKNOWN_CONSUMER;

        // Stale generation means a rebalance occurred since this consumer joined.
        if (generationId != group.generationId) return HeartbeatResult.REBALANCE_IN_PROGRESS;

        m.lastHeartbeatMs = System.currentTimeMillis(); // volatile write, no lock needed
        return HeartbeatResult.OK;
    }

    // -------------------------------------------------------------------------
    // Offset management
    // -------------------------------------------------------------------------

    public OffsetCommitResult commitOffset(String groupId, String consumerId,
                                           int generationId, int partitionId, long offset) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null)                         return OffsetCommitResult.UNKNOWN_GROUP;
        if (!group.members.containsKey(consumerId)) return OffsetCommitResult.UNKNOWN_CONSUMER;
        if (generationId != group.generationId)    return OffsetCommitResult.WRONG_GENERATION;
        if (partitionId < 0 || partitionId >= group.numPartitions)
                                                   return OffsetCommitResult.INVALID_PARTITION;
        group.committedOffsets.set(partitionId, offset); // O(1), lock-free CAS
        return OffsetCommitResult.OK;
    }

    /**
     * Returns the last committed offset for a partition, or -1 if none.
     * Returns Long.MIN_VALUE if the group or partition is unknown.
     */
    public long fetchOffset(String groupId, int partitionId) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null) return Long.MIN_VALUE;
        if (partitionId < 0 || partitionId >= group.numPartitions) return Long.MIN_VALUE;
        return group.committedOffsets.get(partitionId);
    }

    public ConsumerGroup getGroup(String groupId) {
        return groups.get(groupId);
    }

    // -------------------------------------------------------------------------
    // Heartbeat reaper
    // -------------------------------------------------------------------------

    private void evictDeadConsumers() {
        long now = System.currentTimeMillis();
        for (ConsumerGroup group : groups.values()) {
            List<String> dead = new ArrayList<>();
            for (var entry : group.members.entrySet()) {
                if (!entry.getValue().isAlive(now)) dead.add(entry.getKey());
            }
            if (dead.isEmpty()) continue;

            group.rebalanceLock.lock();
            try {
                for (String id : dead) {
                    group.members.remove(id);
                    log.info("Evicted dead consumer: " + id + " from group: " + group.groupId);
                }
                group.rebalance();
            } finally {
                group.rebalanceLock.unlock();
            }
        }
    }

    @Override
    public void close() {
        reaper.shutdownNow();
    }

    // -------------------------------------------------------------------------
    // Result types (avoid throwing exceptions for control flow)
    // -------------------------------------------------------------------------

    public record JoinResult(int generationId, int[] assignedPartitions) {}

    public enum LeaveResult        { OK, UNKNOWN_GROUP, UNKNOWN_CONSUMER }
    public enum HeartbeatResult    { OK, UNKNOWN_GROUP, UNKNOWN_CONSUMER, REBALANCE_IN_PROGRESS }
    public enum OffsetCommitResult { OK, UNKNOWN_GROUP, UNKNOWN_CONSUMER, WRONG_GENERATION, INVALID_PARTITION }
}
