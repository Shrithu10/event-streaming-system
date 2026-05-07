package com.eventstream.broker.metrics;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Background thread that snapshots BrokerMetrics every 5 seconds and logs
 * a human-readable summary.  Delta counters show per-interval throughput so
 * operators can spot trends at a glance without correlating absolute numbers.
 *
 * Output example (logged at INFO):
 *   [METRICS] produce: 12430 msg/s  12.4 MB/s  p50=312μs  p99=1.8ms
 *             fetch:   8210 msg/s   8.2 MB/s   p50=180μs  p99=920μs
 *             wakeup:  fired=12430  saved=49720  batch-ratio=80%
 */
public final class MetricsReporter {

    private static final Logger log = Logger.getLogger(MetricsReporter.class.getName());

    private static final int INTERVAL_SECS = 5;

    private final BrokerMetrics metrics;
    private final ScheduledExecutorService scheduler;

    // Previous-snapshot values for delta computation
    private long lastAppendCount  = 0;
    private long lastAppendBytes  = 0;
    private long lastFetchCount   = 0;
    private long lastFetchBytes   = 0;
    private long lastWakeupFired  = 0;
    private long lastWakeupSaved  = 0;

    public MetricsReporter(BrokerMetrics metrics) {
        this.metrics   = metrics;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-reporter");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::report, INTERVAL_SECS, INTERVAL_SECS, TimeUnit.SECONDS);
        log.info("MetricsReporter started (interval=" + INTERVAL_SECS + "s)");
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    // -------------------------------------------------------------------------

    private void report() {
        try {
            long nowAppendCount = metrics.appendCount();
            long nowAppendBytes = metrics.appendBytes();
            long nowFetchCount  = metrics.fetchCount();
            long nowFetchBytes  = metrics.fetchBytes();
            long nowFired       = metrics.wakeupFired();
            long nowSaved       = metrics.wakeupSaved();

            long dAppendCount = nowAppendCount - lastAppendCount;
            long dAppendBytes = nowAppendBytes - lastAppendBytes;
            long dFetchCount  = nowFetchCount  - lastFetchCount;
            long dFetchBytes  = nowFetchBytes  - lastFetchBytes;
            long dFired       = nowFired        - lastWakeupFired;
            long dSaved       = nowSaved        - lastWakeupSaved;

            lastAppendCount = nowAppendCount;
            lastAppendBytes = nowAppendBytes;
            lastFetchCount  = nowFetchCount;
            lastFetchBytes  = nowFetchBytes;
            lastWakeupFired = nowFired;
            lastWakeupSaved = nowSaved;

            long appendMsgPerSec   = dAppendCount / INTERVAL_SECS;
            long appendMBPerSec    = dAppendBytes / INTERVAL_SECS / (1024 * 1024);
            long fetchMsgPerSec    = dFetchCount  / INTERVAL_SECS;
            long fetchMBPerSec     = dFetchBytes  / INTERVAL_SECS / (1024 * 1024);

            long totalWakeup   = dFired + dSaved;
            int  batchPct      = totalWakeup == 0 ? 0 : (int)(dSaved * 100 / totalWakeup);

            LatencyHistogram al = metrics.appendLatency();
            LatencyHistogram fl = metrics.fetchLatency();

            log.info(String.format(
                    "[METRICS] produce: %6d msg/s  %4d MB/s  p50=%s  p99=%s%n" +
                    "          fetch:   %6d msg/s  %4d MB/s  p50=%s  p99=%s%n" +
                    "          wakeup:  fired=%d  saved=%d  batch-ratio=%d%%",
                    appendMsgPerSec, appendMBPerSec,
                    fmtNs(al.percentileNs(0.50)), fmtNs(al.percentileNs(0.99)),
                    fetchMsgPerSec,  fetchMBPerSec,
                    fmtNs(fl.percentileNs(0.50)), fmtNs(fl.percentileNs(0.99)),
                    dFired, dSaved, batchPct));
        } catch (Exception e) {
            log.warning("MetricsReporter error: " + e.getMessage());
        }
    }

    private static String fmtNs(long ns) {
        if (ns < 1_000)            return ns        + "ns";
        if (ns < 1_000_000)        return (ns/1_000)         + "μs";
        if (ns < 1_000_000_000L)   return (ns/1_000_000)     + "ms";
        return (ns/1_000_000_000L) + "s";
    }
}
