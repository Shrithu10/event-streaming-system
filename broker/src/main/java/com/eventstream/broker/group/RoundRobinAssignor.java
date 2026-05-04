package com.eventstream.broker.group;

import java.util.List;

/**
 * Stateless round-robin partition assignor.
 *
 * Given a sorted list of consumer ids and a partition count, assigns
 * partition i to member[i % memberCount].  Sorting by id makes the result
 * deterministic and reproducible — any node computing the same input arrives
 * at the same assignment, which is essential for Phase 4 replication.
 *
 * Example: 4 partitions, members = [A, B, C] (sorted)
 *   partition 0 → A  (0 % 3 = 0)
 *   partition 1 → B  (1 % 3 = 1)
 *   partition 2 → C  (2 % 3 = 2)
 *   partition 3 → A  (3 % 3 = 0)
 */
public final class RoundRobinAssignor {

    private RoundRobinAssignor() {}

    /**
     * @param sortedMembers consumer ids sorted lexicographically (caller's responsibility)
     * @param numPartitions total number of partitions in the topic
     * @return array of length numPartitions where element i is the consumer id that owns partition i
     */
    public static String[] assign(List<String> sortedMembers, int numPartitions) {
        String[] owner = new String[numPartitions];
        if (sortedMembers.isEmpty()) return owner; // all null = no owner
        for (int p = 0; p < numPartitions; p++) {
            owner[p] = sortedMembers.get(p % sortedMembers.size());
        }
        return owner;
    }
}
