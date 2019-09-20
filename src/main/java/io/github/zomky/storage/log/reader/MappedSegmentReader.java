package io.github.zomky.storage.log.reader;

import io.github.zomky.storage.StorageException;
import io.github.zomky.storage.log.Segment;
import io.github.zomky.storage.log.SegmentHeader;
import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.LogEntry;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Supplier;

import static io.github.zomky.storage.RaftStorageUtils.getInt;
import static io.github.zomky.storage.RaftStorageUtils.openChannel;
import static io.github.zomky.storage.log.serializer.LogEntrySerializer.deserialize;

// TODO should be shared with other readers and each instance use its own sliced buffer ??
public class MappedSegmentReader extends AbstractSegmentReader {

    private Segment segment;
    private FileChannel segmentChannel;
    private FileChannel segmentIndexChannel;
    private MappedByteBuffer segmentBuffer;
    private int nextIndex;
    private Supplier<Long> currentMaxIndexSupplier;

    public MappedSegmentReader(Segment segment) {
        this(segment, segment.getFirstIndex(), () -> Long.MAX_VALUE);
    }

    public MappedSegmentReader(Segment segment, long index) {
        this(segment, index, () -> Long.MAX_VALUE);
    }

    public MappedSegmentReader(Segment segment, Supplier<Long> currentMaxIndexSupplier) {
        this(segment, segment.getFirstIndex(), currentMaxIndexSupplier);
    }

    public MappedSegmentReader(Segment segment, long index, Supplier<Long> currentMaxIndexSupplier) {
        this.segment = segment;
        this.segmentChannel = openChannel(segment.getSegmentPath());
        this.segmentIndexChannel = openChannel(segment.getSegmentIndexPath());
        this.currentMaxIndexSupplier = currentMaxIndexSupplier;
        try {
            int size = segment.getSegmentHeader().getSegmentSize();
            this.segmentBuffer = segmentChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
        } catch (IOException e) {
            throw new StorageException(e);
        }
        reset(index);
    }

    @Override
    protected void readNext() {
        if (currentMaxIndexSupplier.get() < segment.getFirstIndex() + nextIndex - 1) {
            return;
        }
        try {
            segmentBuffer.mark();
            int entrySize = segmentBuffer.getInt();
            if (entrySize == 0) {
                this.next = null;
                segmentBuffer.reset();
                return;
            }
            LogEntry logEntry = deserialize(segmentBuffer, entrySize);
            next = new IndexedLogEntry(logEntry, segment.getFirstIndex() -1 + nextIndex++, entrySize);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    private void resetLocal(int localIndex) {
        int position = getInt(segmentIndexChannel, (localIndex-1) * Integer.BYTES);
        segmentBuffer.position(Math.max(position, SegmentHeader.SIZE));
        this.nextIndex = localIndex;
        this.current = null;
        this.next = null;
    }

    @Override
    public void close()  {
        try {
            PlatformDependent.freeDirectBuffer(segmentBuffer);
            segmentChannel.close();
            segmentIndexChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
