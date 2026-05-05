package com.eventstream.broker.cluster;

/** Composite key used in ConcurrentHashMap lookups for topic+partition pairs. */
public record PartitionKey(String topic, int partitionId) {}
