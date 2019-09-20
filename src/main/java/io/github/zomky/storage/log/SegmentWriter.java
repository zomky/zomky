package io.github.zomky.storage.log;

import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.LogEntry;
import io.github.zomky.storage.log.serializer.LogEntrySerializer;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.zomky.storage.RaftStorageUtils.mapReadWrite;
import static io.github.zomky.storage.RaftStorageUtils.openChannel;

public class SegmentWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentWriter.class);

    private final Segment segment;
    private FileChannel segmentChannel;
    private MappedByteBuffer mappedSegmentBuffer;
    private ByteBuffer segmentBuffer;
    private AtomicBoolean notReleased = new AtomicBoolean(true);

    private SegmentIndexWriter segmentIndexWriter;

    private long lastLogEntryIndex;

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

    public IndexedLogEntry appendEntry(LogEntry logEntry) {
        int position = mappedSegmentBuffer.position();
        int messageLengthSize = Integer.BYTES;

        if (position + messageLengthSize > mappedSegmentBuffer.limit()) {
            throw new BufferOverflowException();
        }

        mappedSegmentBuffer.position(position + messageLengthSize);
        int entrySize = LogEntrySerializer.serialize(logEntry, mappedSegmentBuffer);
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
                mappedSegmentBuffer.force();
                PlatformDependent.freeDirectBuffer(mappedSegmentBuffer);
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

    public long truncateFromIndex(long index) {
        // TODO handle log readers
        if (index <= lastLogEntryIndex) {
            int localIndex = (int) (index - segment.getFirstIndex() + 1);
            segmentIndexWriter.truncateFromIndex(localIndex - 1);
            rewind(localIndex - 1);
        }
        return lastLogEntryIndex;
    }

    public void flush() {
        mappedSegmentBuffer.force();
    }
}
