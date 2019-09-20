package io.github.zomky.storage.log.reader;

import io.github.zomky.storage.StorageException;
import io.github.zomky.storage.log.Segment;
import io.github.zomky.storage.log.SegmentHeader;
import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.LogEntry;
import io.github.zomky.utils.Preconditions;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Supplier;

import static io.github.zomky.storage.RaftStorageUtils.getInt;
import static io.github.zomky.storage.RaftStorageUtils.openChannel;
import static io.github.zomky.storage.log.serializer.LogEntrySerializer.deserialize;

public class ChunkSegmentReader extends AbstractSegmentReader {

    private Segment segment;
    private FileChannel segmentChannel;
    private FileChannel segmentIndexChannel;
    private ByteBuffer segmentBuffer;

    private int nextIndex;
    private int chunkSize;

    private long nextPosition;
    private Supplier<Long> currentMaxIndexSupplier;

    public ChunkSegmentReader(Segment segment, int chunkSize) {
        this(segment, segment.getFirstIndex(), chunkSize);
    }

    public ChunkSegmentReader(Segment segment, int chunkSize, Supplier<Long> currentMaxIndexSupplier) {
        this(segment, segment.getFirstIndex(), chunkSize, currentMaxIndexSupplier);
    }

    public ChunkSegmentReader(Segment segment, long index, int chunkSize) {
        this(segment, index, chunkSize, () -> Long.MAX_VALUE);
    }

    public ChunkSegmentReader(Segment segment, long index, int chunkSize, Supplier<Long> currentMaxIndexSupplier) {
        Preconditions.checkState(index >= segment.getFirstIndex());
        this.chunkSize = chunkSize;
        this.segment = segment;
        this.segmentChannel = openChannel(segment.getSegmentPath());
        this.segmentIndexChannel = openChannel(segment.getSegmentIndexPath());
        this.currentMaxIndexSupplier = currentMaxIndexSupplier;
        this.segmentBuffer = ByteBuffer.allocateDirect(chunkSize);
        reset(index);
    }

    @Override
    protected void readNext() {
        if (currentMaxIndexSupplier.get() < segment.getFirstIndex() + nextIndex - 1) {
            return;
        }
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
                    segmentBuffer.clear();
                    next = null;
                    return;
                }
            }

            if (entrySize > remaining - Integer.BYTES) {
                segmentBuffer.reset();
                ByteBuffer slice = segmentBuffer.slice();
                segmentBuffer.clear();
                segmentBuffer.put(slice);
                segmentChannel.read(segmentBuffer, nextPosition + remaining);
                segmentBuffer.flip();
                entrySize = segmentBuffer.getInt();
                if (entrySize == 0) {
                    segmentBuffer.clear();
                    next = null;
                    return;
                }
                if (entrySize > chunkSize) { // entry bigger than chunk size
                    PlatformDependent.freeDirectBuffer(segmentBuffer);
                    chunkSize = entrySize;
                    segmentBuffer = ByteBuffer.allocateDirect(entrySize);
                    segmentChannel.read(segmentBuffer, nextPosition + Integer.BYTES);
                    segmentBuffer.flip();
                }
            }
            readEntry(segmentBuffer, entrySize);

        } catch (BufferUnderflowException e) {
            next = null;
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    private void readEntry(ByteBuffer buffer, int entrySize) {
        LogEntry logEntry = deserialize(buffer, entrySize);
        next = new IndexedLogEntry(logEntry, segment.getFirstIndex() -1 + nextIndex++, entrySize);
        nextPosition = nextPosition + entrySize + Integer.BYTES;
    }

    @Override
    public long getCurrentIndex() {
        if (current != null) {
            return current.getIndex();
        }
        return nextIndex + segment.getFirstIndex() - 1;
    }

    @Override
    public void reset(long index) {
        resetLocal((int) (index - segment.getFirstIndex() + 1));
    }

    private void resetLocal(int nextEntry) {
        try {
            segmentBuffer.clear();
            int position = getInt(segmentIndexChannel, (nextEntry-1) * Integer.BYTES);
            nextPosition = position > 0 ? position : SegmentHeader.SIZE;
            segmentChannel.position(nextPosition);
            this.nextIndex = nextEntry;
            this.current = null;
            this.next = null;
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void close() {
        try {
            PlatformDependent.freeDirectBuffer(segmentBuffer);
            segmentChannel.close();
            segmentIndexChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
