package com.eventstream.broker.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * Broker-wide performance counters.
 *
 * All writes use LongAdder or LatencyHistogram.record() — both are designed
 * for high-frequency, multi-thread updates with no contention on a hot path.
 *
 * Reads (by MetricsReporter) happen on a background thread every 5 seconds and
 * are inherently weakly consistent; a few counts off across the snapshot window
 * is acceptable for operational metrics.
 *
 * Singleton — obtain via BrokerMetrics.get().
 */
public final class BrokerMetrics {

    private static final BrokerMetrics INSTANCE = new BrokerMetrics();

    // ---- Produce path ----
    private final LongAdder       appendCount   = new LongAdder();
    private final LongAdder       appendBytes   = new LongAdder();
    private final LatencyHistogram appendLatency = new LatencyHistogram();

    // ---- Fetch path ----
    private final LongAdder       fetchCount    = new LongAdder();
    private final LongAdder       fetchBytes    = new LongAdder();
    private final LatencyHistogram fetchLatency  = new LatencyHistogram();

    // ---- Selector wakeup batching ----
    // wakeupFired  = selector.wakeup() calls actually issued
    // wakeupSaved  = calls avoided because another worker already queued one
    private final LongAdder wakeupFired = new LongAdder();
    private final LongAdder wakeupSaved = new LongAdder();

    private BrokerMetrics() {}

    public static BrokerMetrics get() { return INSTANCE; }

    // ---- Record methods (called on hot path) ----

    public void recordAppend(int bytes, long latencyNs) {
        appendCount.increment();
        appendBytes.add(bytes);
        appendLatency.record(latencyNs);
    }

    public void recordFetch(int bytes, long latencyNs) {
        fetchCount.increment();
        fetchBytes.add(bytes);
        fetchLatency.record(latencyNs);
    }

    /** @param saved true if the wakeup() call was batched (skipped), false if it was issued. */
    public void recordWakeup(boolean saved) {
        if (saved) wakeupSaved.increment();
        else       wakeupFired.increment();
    }

    // ---- Snapshot accessors (called by MetricsReporter) ----

    public long appendCount()   { return appendCount.sum(); }
    public long appendBytes()   { return appendBytes.sum(); }
    public long fetchCount()    { return fetchCount.sum(); }
    public long fetchBytes()    { return fetchBytes.sum(); }
    public long wakeupFired()   { return wakeupFired.sum(); }
    public long wakeupSaved()   { return wakeupSaved.sum(); }

    public LatencyHistogram appendLatency() { return appendLatency; }
    public LatencyHistogram fetchLatency()  { return fetchLatency; }
}
