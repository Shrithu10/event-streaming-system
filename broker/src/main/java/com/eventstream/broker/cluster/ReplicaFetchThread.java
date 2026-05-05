package com.eventstream.broker.cluster;

import com.eventstream.broker.storage.LogEntry;
import com.eventstream.broker.topic.Partition;
import com.eventstream.common.protocol.ClusterConfig;
import com.eventstream.common.protocol.ErrorCode;
import com.eventstream.common.protocol.RequestType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * Background thread that continuously pulls new records from the leader for
 * one partition and appends them to the local (follower) log.
 *
 * Pull-based design (same as Kafka):
 *   The follower drives the fetch rate, which naturally provides back-pressure:
 *   a slow follower never blocks the leader.  The leader's ReplicaFetchHandler
 *   uses the fetchOffset in each request to track how far each follower has
 *   caught up, which it uses to advance the High Watermark.
 *
 * Failure detection and leader election:
 *   After FAILURE_THRESHOLD consecutive I/O failures (each separated by
 *   FAILURE_BACKOFF_MS), the thread notifies LeaderElector.  The elector
 *   promotes this broker only if it is first in the follower list
 *   (deterministic tie-break, preventing split-brain in a 2-broker cluster).
 *
 * REPLICA_FETCH request wire format (after the 4-byte length prefix):
 *   [1B  type = 0x09]
 *   [2B  topicLen][N  topic]
 *   [4B  partitionId]
 *   [4B  followerBrokerId]     — lets the leader update ReplicaState
 *   [8B  fetchOffset]          — next byte offset the follower needs
 *   [4B  maxBytes]
 */
public final class ReplicaFetchThread implements Runnable {

    private static final Logger log = Logger.getLogger(ReplicaFetchThread.class.getName());

    private static final int FETCH_INTERVAL_MS  = 50;    // sleep when log tail is caught up
    private static final int MAX_BYTES          = 1 << 20; // 1 MiB per fetch
    private static final int FAILURE_THRESHOLD  = 5;     // consecutive failures → election
    private static final int FAILURE_BACKOFF_MS = 1_000;

    private final int                    localBrokerId;
    private final String                 topic;
    private final int                    partitionId;
    private final ClusterConfig.BrokerInfo leaderInfo;
    private final Partition              localPartition;
    private final LeaderElector          elector;

    private volatile boolean running = true;
    private long             fetchOffset;       // next byte offset to request
    private ReplicaConnection conn;
    private int              consecutiveFailures = 0;

    public ReplicaFetchThread(int localBrokerId, String topic, int partitionId,
                               ClusterConfig.BrokerInfo leaderInfo,
                               Partition localPartition, LeaderElector elector) {
        this.localBrokerId  = localBrokerId;
        this.topic          = topic;
        this.partitionId    = partitionId;
        this.leaderInfo     = leaderInfo;
        this.localPartition = localPartition;
        this.elector        = elector;
        // Start from the end of the local log to avoid re-fetching records
        // already present from a previous run.
        this.fetchOffset    = localPartition.writePosition();
    }

    @Override
    public void run() {
        log.info("Replica fetch thread started: " + topic + "/" + partitionId
                + " → leader " + leaderInfo);

        while (running) {
            try {
                ensureConnected();
                List<LogEntry> records = fetchFromLeader();

                if (records.isEmpty()) {
                    consecutiveFailures = 0;
                    Thread.sleep(FETCH_INTERVAL_MS);
                    continue;
                }

                for (LogEntry entry : records) {
                    localPartition.append(entry.payload);
                }

                // Advance cursor to the byte position after the last record.
                LogEntry last = records.get(records.size() - 1);
                fetchOffset = last.offset + 4 + last.payload.length;
                consecutiveFailures = 0;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                closeConnection();
                consecutiveFailures++;
                log.warning("Replica fetch failed [" + topic + "/" + partitionId + "] "
                        + "attempt=" + consecutiveFailures + ": " + e.getMessage());

                if (consecutiveFailures >= FAILURE_THRESHOLD) {
                    elector.reportLeaderFailure(topic, partitionId, leaderInfo.brokerId());
                    break; // ReplicationManager.onLeaderPromotion() will restart as leader
                }

                try {
                    Thread.sleep(FAILURE_BACKOFF_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        closeConnection();
        log.info("Replica fetch thread stopped: " + topic + "/" + partitionId);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void ensureConnected() throws IOException {
        if (conn == null) {
            conn = new ReplicaConnection(leaderInfo.host(), leaderInfo.port());
            log.fine("Replica connected to " + leaderInfo + " for " + topic + "/" + partitionId);
        }
    }

    private List<LogEntry> fetchFromLeader() throws IOException {
        conn.send(buildRequest());
        ByteBuffer resp = conn.receive();
        /* type = */ resp.get();
        byte error = resp.get();

        if (error == ErrorCode.NOT_LEADER) {
            throw new IOException("Broker " + leaderInfo.brokerId() + " is no longer leader");
        }
        if (error != ErrorCode.NONE) {
            return Collections.emptyList(); // e.g. TOPIC_NOT_FOUND before topic is created
        }

        /* leaderEndOffset = */ resp.getLong(); // informational; not used for HW here
        int count = resp.getInt();
        if (count == 0) return Collections.emptyList();

        List<LogEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long   offset  = resp.getLong();
            int    len     = resp.getInt();
            byte[] payload = new byte[len];
            resp.get(payload);
            entries.add(new LogEntry(offset, payload));
        }
        return entries;
    }

    private ByteBuffer buildRequest() {
        byte[] tb      = topic.getBytes(StandardCharsets.UTF_8);
        int    bodyLen = 1 + 2 + tb.length + 4 + 4 + 8 + 4;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.REPLICA_FETCH);
        buf.putShort((short) tb.length); buf.put(tb);
        buf.putInt(partitionId);
        buf.putInt(localBrokerId);       // tells leader which ReplicaState to update
        buf.putLong(fetchOffset);
        buf.putInt(MAX_BYTES);
        buf.flip();
        return buf;
    }

    private void closeConnection() {
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    public void stop() { running = false; }
}
