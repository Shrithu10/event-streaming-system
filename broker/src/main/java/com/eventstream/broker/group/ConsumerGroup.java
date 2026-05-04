package com.eventstream.broker.group;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * All state for one consumer group.
 *
 * Thread-safety model:
 *
 *   rebalanceLock  — held whenever the assignment is being recomputed (join,
 *                    leave, heartbeat eviction).  The lock duration is short:
 *                    it covers only in-memory operations, no disk I/O.
 *
 *   partitionOwner — volatile array reference.  The rebalance writes a brand-
 *                    new array and then does a single volatile store.  Readers
 *                    (validateFetch) do a single volatile load and then access
 *                    the immutable snapshot.  No lock needed for reads.
 *
 *   committedOffsets — AtomicLongArray.  Per-partition set() is a single CAS.
 *                    Multiple consumers committing different partitions
 *                    concurrently have zero contention.
 *
 *   generationId   — volatile int.  Incremented under rebalanceLock; read
 *                    without the lock on the fetch hot path.
 *
 *   state          — volatile enum.  Set under rebalanceLock; read by workers
 *                    for fast rejection of fetches during rebalance.
 */
public final class ConsumerGroup {

    public final String groupId;
    public final String topicName;
    public final int    numPartitions;

    public volatile GroupState     state        = GroupState.EMPTY;
    public volatile int            generationId = 0;
    public volatile String[]       partitionOwner;   // index=partitionId, value=consumerId

    public final ConcurrentHashMap<String, ConsumerMember> members = new ConcurrentHashMap<>();
    public final AtomicLongArray committedOffsets; // -1 = no committed offset yet
    public final ReentrantLock   rebalanceLock = new ReentrantLock();

    public ConsumerGroup(String groupId, String topicName, int numPartitions) {
        this.groupId       = groupId;
        this.topicName     = topicName;
        this.numPartitions = numPartitions;
        this.partitionOwner    = new String[numPartitions];
        this.committedOffsets  = new AtomicLongArray(numPartitions);
        for (int i = 0; i < numPartitions; i++) committedOffsets.set(i, -1L);
    }

    // -------------------------------------------------------------------------
    // Rebalance — called under rebalanceLock
    // -------------------------------------------------------------------------

    /**
     * Recomputes the round-robin assignment over the current member set.
     * Caller MUST hold rebalanceLock.
     */
    public void rebalance() {
        state = GroupState.REBALANCING;

        List<String> sortedIds = new ArrayList<>(members.keySet());
        Collections.sort(sortedIds); // deterministic

        String[] newOwner = RoundRobinAssignor.assign(sortedIds, numPartitions);

        // Rebuild each member's local partition list.
        Map<String, List<Integer>> byMember = new HashMap<>();
        for (String id : sortedIds) byMember.put(id, new ArrayList<>());
        for (int p = 0; p < numPartitions; p++) {
            if (newOwner[p] != null) byMember.get(newOwner[p]).add(p);
        }
        for (var entry : byMember.entrySet()) {
            ConsumerMember m = members.get(entry.getKey());
            if (m != null) {
                m.assignedPartitions = entry.getValue().stream().mapToInt(i -> i).toArray();
            }
        }

        partitionOwner = newOwner; // single volatile write — immediately visible to all threads
        generationId++;
        state = sortedIds.isEmpty() ? GroupState.EMPTY : GroupState.STABLE;
    }

    // -------------------------------------------------------------------------
    // Fetch validation — lock-free hot path
    // -------------------------------------------------------------------------

    public FetchValidation validateFetch(String consumerId, int genId, int partitionId) {
        if (state != GroupState.STABLE)           return FetchValidation.REBALANCING;
        if (genId != generationId)                return FetchValidation.WRONG_GENERATION;
        if (!members.containsKey(consumerId))     return FetchValidation.UNKNOWN_CONSUMER;
        if (partitionId < 0 || partitionId >= numPartitions) return FetchValidation.NOT_ASSIGNED;
        String[] owner = partitionOwner;          // volatile read — single instruction on x86
        if (!consumerId.equals(owner[partitionId])) return FetchValidation.NOT_ASSIGNED;
        return FetchValidation.OK;
    }

    public enum FetchValidation {
        OK, REBALANCING, WRONG_GENERATION, UNKNOWN_CONSUMER, NOT_ASSIGNED
    }
}
