package io.github.pmackowski.rsocket.raft.storage.log.reader;

import io.github.pmackowski.rsocket.raft.storage.StorageException;
import io.github.pmackowski.rsocket.raft.storage.log.Segment;
import io.github.pmackowski.rsocket.raft.storage.log.SegmentHeader;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;

import static io.github.pmackowski.rsocket.raft.storage.RaftStorageUtils.getInt;
import static io.github.pmackowski.rsocket.raft.storage.RaftStorageUtils.openChannel;
import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;

public class ChunkSegmentReader implements SegmentReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkSegmentReader.class);

    private Segment segment;
    private FileChannel segmentChannel;
    private FileChannel segmentIndexChannel;
    private ByteBuffer segmentBuffer;

    private int nextIndex;
    private int chunkSize;

    private IndexedLogEntry current;
    private IndexedLogEntry next;
    private long nextPosition;

    public ChunkSegmentReader(Segment segment, int chunkSize) {
        this(segment, segment.getFirstIndex(), chunkSize);
    }

    public ChunkSegmentReader(Segment segment, long index, int chunkSize) {
        LOGGER.info("reader start {} , index {}", segment, index);
        Preconditions.checkState(index >= segment.getFirstIndex());
        this.chunkSize = chunkSize;
        this.segment = segment;
        this.segmentChannel = openChannel(segment.getSegmentPath());
        this.segmentIndexChannel = openChannel(segment.getSegmentIndexPath());
        this.nextIndex = (int) (index - segment.getFirstIndex() + 1);
        resetLocal(nextIndex);
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            readNext();
        }
        return next != null;
    }

    @Override
    public IndexedLogEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        current = next;
        next = null;
        readNext();
        return current;
    }

    private void readNext() {
        try {
            if (segmentBuffer.position() == 0) {
                segmentChannel.read(segmentBuffer, nextPosition);
                this.segmentBuffer.flip();
                if (segmentBuffer.remaining() == 0) {
                    next = null;
                    return;
                }
            }
            int entrySize = Integer.MAX_VALUE;
            segmentBuffer.mark();
            int remaining = segmentBuffer.remaining();

            if (remaining >= Integer.BYTES) {
                entrySize = segmentBuffer.getInt();
                if (entrySize == 0) {
                    segmentBuffer = ByteBuffer.allocate(chunkSize);
                    next = null;
                    return;
                }
            }

            if (entrySize > remaining - Integer.BYTES) {
                segmentBuffer.reset();
                ByteBuffer slice = segmentBuffer.slice();
                int currentChunkSize = chunkSize + slice.capacity();
                segmentBuffer = ByteBuffer.allocate(currentChunkSize);
                segmentBuffer.put(slice);
                segmentChannel.read(segmentBuffer, nextPosition + remaining);
                segmentBuffer.flip();
                entrySize = segmentBuffer.getInt();
                if (entrySize == 0) {
                    segmentBuffer = ByteBuffer.allocate(chunkSize);
                    next = null;
                    return;
                }
                if (entrySize > currentChunkSize - Integer.BYTES) { // entry bigger than chunk size
                    segmentBuffer = ByteBuffer.allocate(entrySize);
                    segmentChannel.read(segmentBuffer, nextPosition + Integer.BYTES);
                    segmentBuffer.flip();
                }
            }

            LogEntry logEntry = deserialize(segmentBuffer, entrySize);
            next = new IndexedLogEntry(logEntry, segment.getFirstIndex() -1 + nextIndex++, entrySize);
            nextPosition = nextPosition + entrySize + Integer.BYTES;
        } catch (BufferUnderflowException e) {
            next = null;
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void reset(long index) {
        resetLocal((int) (index - segment.getFirstIndex() + 1));
    }

    private void resetLocal(int nextEntry) {
        try {
            segmentBuffer = ByteBuffer.allocate(chunkSize);
            int position = getInt(segmentIndexChannel, (nextEntry-1) * Integer.BYTES);
            nextPosition = position > 0 ? position : SegmentHeader.SIZE;
            segmentChannel.position(nextPosition);
            this.nextIndex = nextEntry;
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void close() {
        try {
            segmentChannel.close();
            segmentIndexChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
