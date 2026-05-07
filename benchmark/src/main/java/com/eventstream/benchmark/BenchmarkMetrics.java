package com.eventstream.benchmark;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Client-side benchmark metrics — same log2 histogram design as the broker's
 * LatencyHistogram, kept standalone so the benchmark module has no compile
 * dependency on broker internals.
 *
 * All writes use LongAdder / AtomicLongArray (no contention under concurrent
 * ProducerTask / ConsumerTask threads).  Reads happen once at the end of the
 * run on the main thread.
 */
public final class BenchmarkMetrics {

    private static final int BUCKETS = 64;

    // ---- Produce side ----
    private final LongAdder       sentCount  = new LongAdder();
    private final LongAdder       sentBytes  = new LongAdder();
    private final LongAdder       sendErrors = new LongAdder();
    private final AtomicLongArray sendHist   = new AtomicLongArray(BUCKETS); // send RTT ns

    // ---- Consume side ----
    private final LongAdder       recvCount  = new LongAdder();
    private final LongAdder       recvBytes  = new LongAdder();
    private final LongAdder       recvErrors = new LongAdder();

    // ---- Recording ----

    public void recordSend(int bytes, long latencyNs) {
        sentCount.increment();
        sentBytes.add(bytes);
        sendHist.incrementAndGet(bucket(latencyNs));
    }

    public void recordSendError() { sendErrors.increment(); }

    public void recordReceive(int bytes) {
        recvCount.increment();
        recvBytes.add(bytes);
    }

    public void recordReceiveError() { recvErrors.increment(); }

    // ---- Accessors ----

    public long sentCount()  { return sentCount.sum(); }
    public long sentBytes()  { return sentBytes.sum(); }
    public long sendErrors() { return sendErrors.sum(); }
    public long recvCount()  { return recvCount.sum(); }
    public long recvBytes()  { return recvBytes.sum(); }
    public long recvErrors() { return recvErrors.sum(); }

    public long sendP50Ns() { return percentile(0.50); }
    public long sendP95Ns() { return percentile(0.95); }
    public long sendP99Ns() { return percentile(0.99); }

    // ---- Helpers ----

    private int bucket(long nanos) {
        if (nanos <= 0) return 0;
        return Math.min(BUCKETS - 1, 63 - Long.numberOfLeadingZeros(nanos));
    }

    private long percentile(double p) {
        long total = sentCount.sum();
        if (total == 0) return 0;
        long target     = (long) Math.ceil(total * p);
        long cumulative = 0;
        for (int k = 0; k < BUCKETS; k++) {
            cumulative += sendHist.get(k);
            if (cumulative >= target) return k == 0 ? 0L : (1L << k);
        }
        return 1L << (BUCKETS - 1);
    }

    public static String fmtNs(long ns) {
        if (ns < 1_000)          return ns              + " ns";
        if (ns < 1_000_000)      return (ns / 1_000)    + " μs";
        if (ns < 1_000_000_000L) return (ns / 1_000_000) + " ms";
        return (ns / 1_000_000_000L) + " s";
    }
}
