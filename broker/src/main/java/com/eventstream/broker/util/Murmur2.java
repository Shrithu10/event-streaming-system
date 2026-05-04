package com.eventstream.broker.util;

/**
 * MurmurHash2 (32-bit) — the same algorithm Kafka uses for producer key routing.
 *
 * Compared to Java's String.hashCode():
 *   - Uniform bit distribution across all 32 bits (hashCode has poor low-bit quality)
 *   - Avalanche effect: one input bit flip changes ~50% of output bits
 *   - O(len) time, no allocations
 *
 * This matters for partition balance: poor hashing causes hot partitions even
 * with many keys.
 */
public final class Murmur2 {

    private static final int SEED = 0x9747b28c;
    private static final int M    = 0x5bd1e995;
    private static final int R    = 24;

    private Murmur2() {}

    public static int hash(byte[] data) {
        return hash(data, SEED);
    }

    public static int hash(byte[] data, int seed) {
        int len = data.length;
        int h   = seed ^ len;
        int i   = 0;

        while (len >= 4) {
            int k = (data[i]     & 0xFF)
                  | ((data[i+1] & 0xFF) << 8)
                  | ((data[i+2] & 0xFF) << 16)
                  | ((data[i+3] & 0xFF) << 24);
            k *= M;
            k ^= k >>> R;
            k *= M;
            h *= M;
            h ^= k;
            i   += 4;
            len -= 4;
        }

        // Handle remaining bytes (fall-through intentional — this is Murmur2's tail mix)
        switch (len) {
            case 3: h ^= (data[i + 2] & 0xFF) << 16; // fall through
            case 2: h ^= (data[i + 1] & 0xFF) << 8;  // fall through
            case 1: h ^= (data[i]     & 0xFF);
                    h *= M;
        }

        h ^= h >>> 13;
        h *= M;
        h ^= h >>> 15;
        return h;
    }
}
