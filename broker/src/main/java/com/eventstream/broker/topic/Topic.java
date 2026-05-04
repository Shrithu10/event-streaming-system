package com.eventstream.broker.topic;

import com.eventstream.broker.util.Murmur2;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Immutable partition array for a topic plus routing logic.
 *
 * The partition array is fixed at creation time (Phase 2 does not support
 * partition expansion after topic creation — that requires coordinator
 * consensus and is a Phase 4+ concern).
 *
 * Routing strategy:
 *   key present  → murmur2(key) & MAX_VALUE % numPartitions
 *   key absent   → round-robin via AtomicLong (no CAS contention path)
 *
 * Both operations are O(1) with no locks on the hot path.
 */
public final class Topic {

    public final String      name;
    public final Partition[] partitions; // index == partitionId

    private final AtomicLong roundRobin = new AtomicLong();

    public Topic(String name, Partition[] partitions) {
        this.name       = name;
        this.partitions = partitions;
    }

    public int numPartitions() {
        return partitions.length;
    }

    /**
     * Routes a produce request to a partition.
     *
     * @param key routing key; null or empty triggers round-robin
     * @return the target Partition (never null — caller owns the topic)
     */
    public Partition route(byte[] key) {
        int id;
        if (key == null || key.length == 0) {
            // getAndIncrement is a single fetch-and-add (lock-free on x86/ARM).
            // The modulo keeps the counter from ever wrapping into a negative index.
            id = (int) (roundRobin.getAndIncrement() % partitions.length);
        } else {
            id = (Murmur2.hash(key) & Integer.MAX_VALUE) % partitions.length;
        }
        return partitions[id];
    }

    /**
     * Returns a specific partition by id, or null if out of range.
     * Consumers use this after the broker tells them how many partitions exist.
     */
    public Partition get(int partitionId) {
        if (partitionId < 0 || partitionId >= partitions.length) return null;
        return partitions[partitionId];
    }
}
