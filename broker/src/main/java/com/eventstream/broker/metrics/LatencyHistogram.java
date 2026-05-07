package com.eventstream.broker.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Lock-free log2 latency histogram with 64 buckets.
 *
 * Bucket k covers the nanosecond range [2^k, 2^(k+1)).
 * Bucket 0 covers [0, 2) ns (effectively zero-latency measurements).
 * Bucket 63 is the overflow bucket for latencies >= 2^62 ns (impractical but safe).
 *
 * Recording is a single AtomicLongArray CAS-free increment — O(1), no contention.
 * Snapshot reads are weakly consistent (sum/count may lag by one record at query time;
 * this is acceptable for periodic reporting).
 */
public final class LatencyHistogram {

    private static final int BUCKETS = 64;

    private final AtomicLongArray counts = new AtomicLongArray(BUCKETS);
    private final AtomicLong      total  = new AtomicLong(0);  // sum of all nanos recorded
    private final AtomicLong      count  = new AtomicLong(0);

    public void record(long nanos) {
        if (nanos < 0) nanos = 0;
        int bucket = nanos == 0 ? 0 : Math.min(BUCKETS - 1, 63 - Long.numberOfLeadingZeros(nanos));
        counts.incrementAndGet(bucket);
        count.incrementAndGet();
        total.addAndGet(nanos);
    }

    public long count() { return count.get(); }

    /** Mean latency in nanoseconds; 0 if no samples. */
    public long meanNs() {
        long n = count.get();
        return n == 0 ? 0 : total.get() / n;
    }

    /**
     * Approximate percentile in nanoseconds.
     * Returns the lower bound of the bucket at which the cumulative count
     * reaches {@code p}% of total. p=0.99 gives p99, p=0.50 gives p50.
     */
    public long percentileNs(double p) {
        long n = count.get();
        if (n == 0) return 0;
        long target  = (long) Math.ceil(n * p);
        long cumulative = 0;
        for (int k = 0; k < BUCKETS; k++) {
            cumulative += counts.get(k);
            if (cumulative >= target) {
                return k == 0 ? 0L : (1L << k); // lower bound of bucket k
            }
        }
        return 1L << (BUCKETS - 1);
    }

    /** Resets all counts to zero. Used by MetricsReporter to track per-interval deltas. */
    public void reset() {
        for (int k = 0; k < BUCKETS; k++) counts.set(k, 0);
        count.set(0);
        total.set(0);
    }
}
