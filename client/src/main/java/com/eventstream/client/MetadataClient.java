package com.eventstream.client;

import com.eventstream.common.protocol.ClusterConfig;
import com.eventstream.common.protocol.ErrorCode;
import com.eventstream.common.protocol.RequestType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetches and caches cluster metadata from any reachable broker.
 *
 * Sends a METADATA request (just the type byte) and parses METADATA_ACK to
 * produce a {@link ClusterConfig} describing the current broker topology and
 * partition leader assignments.
 *
 * The cached result is refreshed automatically after REFRESH_INTERVAL_MS or
 * whenever the caller explicitly calls {@link #refresh()}.
 *
 * Thread safety: volatile fields for the cache; {@link #refresh()} may be
 * called from multiple threads concurrently — at worst both execute a fetch
 * and the second result overwrites the first (both are valid).
 */
public final class MetadataClient {

    private static final long REFRESH_INTERVAL_MS = 30_000;

    private final String bootstrapHost;
    private final int    bootstrapPort;

    private volatile ClusterConfig cachedConfig;
    private volatile long          lastRefreshMs = 0;

    public MetadataClient(String bootstrapHost, int bootstrapPort) {
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
    }

    /** Returns cached metadata, refreshing if stale or not yet loaded. */
    public ClusterConfig metadata() throws IOException {
        long now = System.currentTimeMillis();
        if (cachedConfig == null || (now - lastRefreshMs) > REFRESH_INTERVAL_MS) {
            refresh();
        }
        return cachedConfig;
    }

    /** Forces a metadata fetch from the bootstrap broker. */
    public void refresh() throws IOException {
        try (BrokerConnection conn = new BrokerConnection(bootstrapHost, bootstrapPort)) {
            conn.send(buildRequest());
            ByteBuffer resp = conn.receive();
            /* type  = */ resp.get();
            byte error = resp.get();
            if (error != ErrorCode.NONE) {
                throw new IOException("METADATA request failed, error=0x"
                        + Integer.toHexString(error & 0xFF));
            }

            // ---- Brokers ----
            int brokerCount = resp.getInt();
            List<ClusterConfig.BrokerInfo> brokers = new ArrayList<>(brokerCount);
            for (int i = 0; i < brokerCount; i++) {
                int    brokerId = resp.getInt();
                int    hostLen  = resp.getShort() & 0xFFFF;
                byte[] hostBytes = new byte[hostLen];
                resp.get(hostBytes);
                int port = resp.getInt();
                brokers.add(new ClusterConfig.BrokerInfo(
                        brokerId, new String(hostBytes, StandardCharsets.UTF_8), port));
            }

            // ---- Assignments ----
            int assignCount = resp.getInt();
            List<ClusterConfig.PartitionAssignment> assignments = new ArrayList<>(assignCount);
            for (int i = 0; i < assignCount; i++) {
                int    topicLen   = resp.getShort() & 0xFFFF;
                byte[] topicBytes = new byte[topicLen];
                resp.get(topicBytes);
                String topic         = new String(topicBytes, StandardCharsets.UTF_8);
                int    partitionId   = resp.getInt();
                int    leaderId      = resp.getInt();
                int    followerCount = resp.getInt();
                int[]  followerIds   = new int[followerCount];
                for (int j = 0; j < followerCount; j++) followerIds[j] = resp.getInt();
                assignments.add(new ClusterConfig.PartitionAssignment(
                        topic, partitionId, leaderId, followerIds));
            }

            this.cachedConfig  = ClusterConfig.of(brokers, assignments);
            this.lastRefreshMs = System.currentTimeMillis();
        }
    }

    private static ByteBuffer buildRequest() {
        ByteBuffer buf = ByteBuffer.allocate(4 + 1);
        buf.putInt(1);
        buf.put(RequestType.METADATA);
        buf.flip();
        return buf;
    }
}
