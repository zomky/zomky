package io.github.zomky.storage.log.reader;

import io.github.zomky.storage.BufferCleaner;
import io.github.zomky.storage.StorageException;
import io.github.zomky.storage.log.Segment;
import io.github.zomky.storage.log.SegmentHeader;
import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static io.github.zomky.storage.RaftStorageUtils.getInt;
import static io.github.zomky.storage.RaftStorageUtils.openChannel;
import static io.github.zomky.storage.log.serializer.LogEntrySerializer.deserialize;

public class MappedSegmentReader implements SegmentReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappedSegmentReader.class);

    private Segment segment;
    private FileChannel segmentChannel;
    private FileChannel segmentIndexChannel;
    private MappedByteBuffer segmentBuffer;
    private int nextIndex;
    private IndexedLogEntry current;
    private IndexedLogEntry next;

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
        if (nextIndex > currentMaxIndexSupplier.get() - segment.getFirstIndex() + 1) {
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
            BufferCleaner.closeDirectBuffer(segmentBuffer);
            segmentChannel.close();
            segmentIndexChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
