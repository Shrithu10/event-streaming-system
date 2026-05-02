package com.eventstream.broker.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * Append-only log segment backed by a single file.
 *
 * On-disk record format:
 *   [4 bytes: payload length][N bytes: payload]
 *
 * The "offset" of a record is its starting byte position in the file.
 * Seeking to an offset is O(1) via positional FileChannel reads.
 *
 * Thread safety:
 *   - Writes are serialized with writeLock.
 *   - Reads are concurrent; FileChannel.read(buf, pos) is safe for multiple
 *     threads per the Java NIO spec.
 */
public final class LogSegment implements Closeable {

    private static final Logger log = Logger.getLogger(LogSegment.class.getName());

    private final FileChannel  channel;
    private final AtomicLong   writePosition;
    private final ReentrantLock writeLock = new ReentrantLock();

    public LogSegment(Path file) throws IOException {
        this.channel = FileChannel.open(
                file,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        // On restart, resume from the existing file end.
        this.writePosition = new AtomicLong(channel.size());
        log.info("LogSegment opened: " + file + " (size=" + channel.size() + ")");
    }

    /**
     * Appends a message to the log.
     *
     * @return byte offset of this record (used as the message offset by callers)
     */
    public long append(byte[] payload) throws IOException {
        // Allocate exactly one record buffer: 4-byte length header + payload.
        // Phase 5 will replace this with pooled / pre-allocated write buffers.
        int recordSize = 4 + payload.length;
        ByteBuffer buf = ByteBuffer.allocate(recordSize);
        buf.putInt(payload.length);
        buf.put(payload);
        buf.flip();

        writeLock.lock();
        try {
            long offset = writePosition.get();
            long pos = offset;
            // Positional writes do not change the channel's internal position.
            // Loop guards against short writes (rare but specified by the API).
            while (buf.hasRemaining()) {
                pos += channel.write(buf, pos);
            }
            writePosition.addAndGet(recordSize);
            return offset;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Reads records starting at startOffset, stopping when maxBytes of
     * (header + payload) data has been accumulated or the end of the log is
     * reached.
     *
     * Returns an empty list if startOffset >= current log size.
     */
    public List<LogEntry> read(long startOffset, int maxBytes) throws IOException {
        long fileSize = writePosition.get(); // snapshot – reads never go past committed writes

        if (startOffset < 0 || startOffset >= fileSize) {
            return Collections.emptyList();
        }

        List<LogEntry> entries = new ArrayList<>();
        long pos = startOffset;
        int accumulated = 0;

        ByteBuffer lenBuf = ByteBuffer.allocate(4);

        while (pos < fileSize && accumulated < maxBytes) {
            // --- Read the 4-byte length header ---
            lenBuf.clear();
            long readPos = pos;
            while (lenBuf.hasRemaining()) {
                int r = channel.read(lenBuf, readPos);
                if (r == -1) return entries; // truncated segment
                readPos += r;
            }
            lenBuf.flip();
            int payloadLen = lenBuf.getInt();

            // Guard against corrupted records.
            if (payloadLen < 0 || payloadLen > 64 * 1024 * 1024) {
                log.warning("Corrupt record at offset " + pos + ": payloadLen=" + payloadLen);
                return entries;
            }

            int recordSize = 4 + payloadLen;

            // Honour maxBytes: if adding this record would exceed the budget
            // and we already have at least one entry, stop here.  If we have
            // nothing yet, return the single (oversized) record so the consumer
            // can always make progress.
            if (accumulated + recordSize > maxBytes && !entries.isEmpty()) {
                break;
            }

            // --- Read the payload ---
            byte[] payload = new byte[payloadLen];
            ByteBuffer payloadBuf = ByteBuffer.wrap(payload);
            long payloadPos = pos + 4;
            while (payloadBuf.hasRemaining()) {
                int r = channel.read(payloadBuf, payloadPos);
                if (r == -1) return entries;
                payloadPos += r;
            }

            entries.add(new LogEntry(pos, payload));
            accumulated += recordSize;
            pos += recordSize;
        }

        return entries;
    }

    /** Current committed write position (i.e. the offset of the next record). */
    public long size() {
        return writePosition.get();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
