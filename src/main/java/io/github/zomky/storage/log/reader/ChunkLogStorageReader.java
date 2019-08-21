package io.github.zomky.storage.log.reader;

import io.github.zomky.storage.StorageException;
import io.github.zomky.storage.log.Segment;
import io.github.zomky.storage.log.Segments;
import io.github.zomky.storage.log.entry.IndexedLogEntry;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

public class ChunkLogStorageReader implements LogStorageReader {

    public static final int CHUNK_SIZE = 8 * 1024;

    private Segments segments;
    private Segment currentSegment;
    private SegmentReader currentSegmentReader;
    private long initialIndex;
    private Supplier<Long> currentMaxIndexSupplier;

    public ChunkLogStorageReader(Segments segments, long index, Supplier<Long> currentMaxIndexSupplier) {
        this.segments = segments;
        this.currentSegment = segments.getSegment(index);
        this.currentSegmentReader = new ChunkSegmentReader(currentSegment, index, CHUNK_SIZE, currentMaxIndexSupplier);
        this.initialIndex = index;
        this.currentMaxIndexSupplier = currentMaxIndexSupplier;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = true;
        if (!currentSegmentReader.hasNext()) {
            Segment nextSegment = segments.getNextSegment(currentSegment.getFirstIndex());
            hasNext = false;
            if (nextSegment != null) {
                currentSegmentReader.close();
                currentSegment = nextSegment;
                currentSegmentReader = new ChunkSegmentReader(currentSegment, CHUNK_SIZE, currentMaxIndexSupplier);
                hasNext = currentSegmentReader.hasNext();
            }
        }
        return hasNext;
    }

    @Override
    public IndexedLogEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return currentSegmentReader.next();
    }

    @Override
    public long getCurrentIndex() {
        return currentSegmentReader.getCurrentIndex();
    }

    @Override
    public void reset() {
        reset(initialIndex);
    }

    @Override
    public void reset(long index) {
        final Segment segment = segments.getSegment(index);
        if (currentSegment.getFirstIndex() != segment.getFirstIndex()) {
            currentSegmentReader.close();
            currentSegment = segment;
            currentSegmentReader = new ChunkSegmentReader(currentSegment, index, CHUNK_SIZE, currentMaxIndexSupplier);
        }
        currentSegmentReader.reset(index);
    }

    @Override
    public void close() {
        try {
            if (currentSegmentReader != null) {
                currentSegmentReader.close();
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

}
