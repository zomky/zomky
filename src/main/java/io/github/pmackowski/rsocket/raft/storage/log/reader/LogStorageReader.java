package io.github.pmackowski.rsocket.raft.storage.log.reader;

import io.github.pmackowski.rsocket.raft.storage.StorageException;
import io.github.pmackowski.rsocket.raft.storage.log.Segment;
import io.github.pmackowski.rsocket.raft.storage.log.Segments;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.ChunkSegmentReader;

import java.nio.ByteBuffer;
import java.util.*;

import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;
import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.serialize;

// TODO
public class LogStorageReader {

    private Segments segments;
    private SegmentReader currentSegmentReader;

    public LogStorageReader(Segments segments, long index) {
        this.segments = segments;
    }

    public synchronized Iterable<ByteBuffer> getRawEntriesByIndex(long indexFrom, long indexTo) {
        try {
            List<ByteBuffer> result = new LinkedList<>();
            Segment currentSegment = segments.getSegment(indexFrom);
            currentSegmentReader = new ChunkSegmentReader(currentSegment, indexFrom, 32);
            long entriesCounter = indexTo - indexFrom + 1;
            while (entriesCounter > 0) {
                boolean hasNext = true;
                if (!currentSegmentReader.hasNext()) {
                    Segment nextSegment = segments.getNextSegment(currentSegment.getFirstIndex());
                    hasNext = false;
                    if (nextSegment != null) {
                        currentSegmentReader.close();
                        currentSegment = nextSegment;
                        currentSegmentReader = new ChunkSegmentReader(currentSegment, indexFrom, 32);
                        hasNext = currentSegmentReader.hasNext();
                    }
                }
                if (hasNext) {
                    IndexedLogEntry entry = currentSegmentReader.next();
                    ByteBuffer byteBuffer = ByteBuffer.allocate(entry.getSize());
                    serialize(entry.getLogEntry(), byteBuffer);
                    byteBuffer.flip();
                    result.add(byteBuffer);
                    entriesCounter--;
                } else {
                    entriesCounter = 0;
                }
            }
            return result;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public synchronized List<IndexedLogEntry> getEntriesByIndex(long indexFrom, long indexTo) {
        try {
            List<IndexedLogEntry> result = new LinkedList<>();
            Segment currentSegment = segments.getSegment(indexFrom);
            currentSegmentReader = new ChunkSegmentReader(currentSegment, indexFrom, 32);
            long entriesCounter = indexTo - indexFrom + 1;
            while (entriesCounter > 0) {
                boolean hasNext = true;
                if (!currentSegmentReader.hasNext()) {
                    Segment nextSegment = segments.getNextSegment(currentSegment.getFirstIndex());
                    hasNext = false;
                    if (nextSegment != null) {
                        currentSegmentReader.close();
                        currentSegment = nextSegment;
                        currentSegmentReader = new ChunkSegmentReader(currentSegment, 32);
                        hasNext = currentSegmentReader.hasNext();
                    }
                }
                if (hasNext) {
                    IndexedLogEntry entry = currentSegmentReader.next();
                    result.add(entry);
                    entriesCounter--;
                } else {
                    entriesCounter = 0;
                }
            }
            return result;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public void close()  {
        try {
            if (currentSegmentReader != null) {
                currentSegmentReader.close();
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

}
