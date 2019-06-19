package io.github.pmackowski.rsocket.raft.storage.log.reader;

import io.github.pmackowski.rsocket.raft.storage.StorageException;
import io.github.pmackowski.rsocket.raft.storage.log.Segment;
import io.github.pmackowski.rsocket.raft.storage.log.Segments;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;

import java.util.NoSuchElementException;

public class ChunkLogStorageReader implements LogStorageReader {

    private Segments segments;
    private Segment currentSegment;
    private SegmentReader currentSegmentReader;
    private long initialIndex;

    public ChunkLogStorageReader(Segments segments) {
        this(segments, 1L);
    }

    public ChunkLogStorageReader(Segments segments, long index) {
        this.segments = segments;
        this.currentSegment = segments.getSegment(index);
        this.currentSegmentReader = new ChunkSegmentReader(currentSegment, index, 8 * 1024);
        this.initialIndex = index;
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
                currentSegmentReader = new ChunkSegmentReader(currentSegment, 8 * 1024);
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
    public void reset() {
        reset(initialIndex);
    }

    @Override
    public void reset(long index) {
        final Segment segment = segments.getSegment(index);
        if (currentSegment.getFirstIndex() != segment.getFirstIndex()) {
            currentSegmentReader.close();
            currentSegment = segment;
            currentSegmentReader = new ChunkSegmentReader(currentSegment, index, 32);
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
