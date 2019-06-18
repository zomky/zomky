package io.github.pmackowski.rsocket.raft.storage.log;

import io.github.pmackowski.rsocket.raft.storage.BufferCleaner;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.pmackowski.rsocket.raft.storage.RaftStorageUtils.*;
import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;
import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.serialize;

public class SegmentWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentWriter.class);

    private final Segment segment;
    private FileChannel segmentChannel;
    private MappedByteBuffer mappedSegmentBuffer;
    private ByteBuffer segmentBuffer;
    private AtomicBoolean notReleased = new AtomicBoolean(true);

    private SegmentIndexWriter segmentIndexWriter;

    private volatile long lastLogEntryIndex;

    public SegmentWriter(Segment segment) {
        this.segment = segment;
        this.segmentIndexWriter = new SegmentIndexWriter(segment);

        this.segmentChannel = openChannel(segment.getSegmentPath());
        this.mappedSegmentBuffer = mapReadWrite(segmentChannel, segment);
        this.segmentBuffer = this.mappedSegmentBuffer.slice();

        int entriesCount = segment.entriesCount();

        rewind(entriesCount);
    }

    private void rewind(int entriesCount) {
        if (entriesCount == 0) {
            this.mappedSegmentBuffer.position(SegmentHeader.SIZE);
        } else {
            int lastIndexPosition = segmentIndexWriter.position(entriesCount);
            int entrySize = segmentBuffer.getInt(lastIndexPosition);
            mappedSegmentBuffer.position(lastIndexPosition + Integer.BYTES + entrySize);
            segmentIndexWriter.rewind(entriesCount);
        }
        lastLogEntryIndex = segment.getFirstIndex() + entriesCount - 1;
    }

    // not thread safe
    public IndexedLogEntry appendEntry(LogEntry logEntry) {
        int position = mappedSegmentBuffer.position();
        int messageLengthSize = Integer.BYTES;

        if (position + messageLengthSize > mappedSegmentBuffer.limit()) {
            throw new BufferOverflowException();
        }

        mappedSegmentBuffer.position(position + messageLengthSize);
        int entrySize = serialize(logEntry, mappedSegmentBuffer);
        mappedSegmentBuffer.putInt(position, entrySize);
        segmentIndexWriter.append(position);
        lastLogEntryIndex = lastLogEntryIndex + 1;
        return new IndexedLogEntry(logEntry, lastLogEntryIndex, entrySize);
    }

    public long getLastLogEntryIndex() {
        return lastLogEntryIndex;
    }

    public void release() {
        if (notReleased.getAndSet(false)) {
            segmentIndexWriter.release();
            try {
                BufferCleaner.closeDirectBuffer(mappedSegmentBuffer);
            } catch (Exception e) {
                LOGGER.error("Failed to unmap direct buffer", e);
            }
            try {
                segmentChannel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
