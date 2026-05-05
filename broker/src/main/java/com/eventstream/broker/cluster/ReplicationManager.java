package com.eventstream.broker.cluster;

import com.eventstream.broker.topic.Partition;
import com.eventstream.broker.topic.TopicManager;
import com.eventstream.common.protocol.ClusterConfig;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Owns the lifecycle of all replication state and threads for this broker.
 *
 * On start():
 *   For each partition where this broker is the leader  → initialise PartitionLeaderState.
 *   For each partition where this broker is a follower  → start ReplicaFetchThread.
 *
 * Thread safety contract:
 *   leaderStates — written only during start() and onLeaderPromotion() (sequential).
 *                  Read by handler workers concurrently via leaderState() — safe because
 *                  ConcurrentHashMap.get() is lock-free.
 *   fetchThreads — written only during start() and onLeaderPromotion() (sequential).
 *                  Read by onLeaderPromotion() — protected by the fact that only one
 *                  LeaderElector callback runs at a time per partition (single fetch thread).
 *
 * Single-node mode: start() is a no-op; isLeader() returns true; highWatermark() returns
 *   Long.MAX_VALUE (no clamping of consumer reads).
 */
public final class ReplicationManager implements Closeable {

    private static final Logger log = Logger.getLogger(ReplicationManager.class.getName());

    private final ClusterMetadata metadata;
    private final TopicManager    topicManager;
    private final ExecutorService replicaPool;

    private final ConcurrentHashMap<PartitionKey, PartitionLeaderState> leaderStates
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PartitionKey, ReplicaFetchThread>   fetchThreads
            = new ConcurrentHashMap<>();

    private LeaderElector elector; // set in start() to break the circular reference

    public ReplicationManager(ClusterMetadata metadata, TopicManager topicManager) {
        this.metadata     = metadata;
        this.topicManager = topicManager;
        this.replicaPool  = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "replica-fetch-" + r.hashCode());
            t.setDaemon(true);
            return t;
        });
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public void start() {
        this.elector = new LeaderElector(metadata.localBrokerId(), metadata, this);

        if (metadata.isSingleNodeMode()) {
            log.info("Single-node mode — replication disabled");
            return;
        }

        for (ClusterConfig.PartitionAssignment a : metadata.config().assignments()) {
            if (a.leaderId() == metadata.localBrokerId()) {
                initLeaderState(a.topic(), a.partitionId(), a.followerIds());
            } else if (isLocalFollower(a)) {
                startFetchThread(a.topic(), a.partitionId(), a.leaderId());
            }
        }
    }

    /** Called by LeaderElector when this broker is elected leader for a partition. */
    public void onLeaderPromotion(String topic, int partitionId) {
        PartitionKey key = new PartitionKey(topic, partitionId);

        ReplicaFetchThread ft = fetchThreads.remove(key);
        if (ft != null) ft.stop();

        // No followers known yet — they will re-connect and their ReplicaState
        // will be added dynamically when their first REPLICA_FETCH arrives.
        initLeaderState(topic, partitionId, new int[0]);

        log.warning("Promotion complete: now leader for " + topic + "/" + partitionId);
    }

    // -------------------------------------------------------------------------
    // Handler-facing API
    // -------------------------------------------------------------------------

    /** True if this broker is currently the leader for the given partition. */
    public boolean isLeader(String topic, int partitionId) {
        return metadata.isLeader(topic, partitionId);
    }

    /**
     * High Watermark for a partition.
     * Returns Long.MAX_VALUE in single-node mode (no clamping of consumer reads).
     */
    public long highWatermark(String topic, int partitionId) {
        if (metadata.isSingleNodeMode()) return Long.MAX_VALUE;
        PartitionLeaderState state = leaderStates.get(new PartitionKey(topic, partitionId));
        return state != null ? state.highWatermark() : Long.MAX_VALUE;
    }

    /**
     * Called by ProduceHandler after a successful local append.
     * Advances the leader's end-offset so HW computation has the correct upper bound.
     */
    public void updateLeaderEndOffset(String topic, int partitionId, long newEndOffset) {
        if (metadata.isSingleNodeMode()) return;
        PartitionLeaderState state = leaderStates.get(new PartitionKey(topic, partitionId));
        if (state != null) state.updateLeaderEndOffset(newEndOffset);
    }

    /**
     * Called by ReplicaFetchHandler when a follower's REPLICA_FETCH arrives.
     * Records the follower's position so HW can advance.
     */
    public void onFollowerFetch(String topic, int partitionId,
                                int followerBrokerId, long followerFetchOffset) {
        PartitionLeaderState state = leaderStates.get(new PartitionKey(topic, partitionId));
        if (state == null) {
            // First fetch from a newly promoted leader's perspective — add replica dynamically.
            Map<Integer, ReplicaState> m = new HashMap<>();
            m.put(followerBrokerId, new ReplicaState(followerBrokerId));
            PartitionLeaderState newState = new PartitionLeaderState(m);
            newState.updateLeaderEndOffset(
                    topicManager.getPartition(topic, partitionId) != null
                            ? topicManager.getPartition(topic, partitionId).writePosition()
                            : 0L);
            leaderStates.putIfAbsent(new PartitionKey(topic, partitionId), newState);
            state = leaderStates.get(new PartitionKey(topic, partitionId));
        }
        if (state != null) state.onFollowerFetch(followerBrokerId, followerFetchOffset);
    }

    public ClusterMetadata      metadata()                                    { return metadata; }
    public PartitionLeaderState leaderState(String topic, int partitionId)    {
        return leaderStates.get(new PartitionKey(topic, partitionId));
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private void initLeaderState(String topic, int partitionId, int[] followerIds) {
        Map<Integer, ReplicaState> replicas = new HashMap<>();
        for (int fid : followerIds) replicas.put(fid, new ReplicaState(fid));

        Partition partition = topicManager.getPartition(topic, partitionId);
        PartitionLeaderState state = new PartitionLeaderState(replicas);
        if (partition != null) state.updateLeaderEndOffset(partition.writePosition());

        leaderStates.put(new PartitionKey(topic, partitionId), state);
        log.info("Leader state initialised for " + topic + "/" + partitionId
                + " (followers=" + followerIds.length + " endOffset="
                + state.leaderEndOffset() + ")");
    }

    private void startFetchThread(String topic, int partitionId, int leaderId) {
        ClusterConfig.BrokerInfo leaderInfo = metadata.config().broker(leaderId).orElse(null);
        if (leaderInfo == null) {
            log.severe("Unknown leader brokerId=" + leaderId + " for " + topic + "/" + partitionId);
            return;
        }
        Partition partition = topicManager.getPartition(topic, partitionId);
        if (partition == null) {
            log.severe("Partition not in TopicManager: " + topic + "/" + partitionId);
            return;
        }
        ReplicaFetchThread thread = new ReplicaFetchThread(
                metadata.localBrokerId(), topic, partitionId, leaderInfo, partition, elector);
        fetchThreads.put(new PartitionKey(topic, partitionId), thread);
        replicaPool.submit(thread);
        log.info("Replica fetch thread started: " + topic + "/" + partitionId
                + " → leader brokerId=" + leaderId);
    }

    private boolean isLocalFollower(ClusterConfig.PartitionAssignment a) {
        for (int f : a.followerIds()) if (f == metadata.localBrokerId()) return true;
        return false;
    }

    @Override
    public void close() {
        fetchThreads.values().forEach(ReplicaFetchThread::stop);
        replicaPool.shutdownNow();
        try {
            replicaPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
