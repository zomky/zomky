package io.github.pmackowski.rsocket.raft.storage.log;

import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.ChunkLogStorageReader;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.BufferOverflowException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

public class LogStorage implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogStorage.class);

    private final Segments segments;
    private SegmentWriter segmentWriter;
    private List<LogStorageReader> logReaders = new CopyOnWriteArrayList<>();
    private IndexedLogEntry lastLogEntry;

    public LogStorage(RaftStorageConfiguration configuration) {
        this.segments = new Segments(configuration, new SegmentsReader(), new SegmentsWriter());
        this.segmentWriter = new SegmentWriter(segments.getLastSegment());
        this.lastLogEntry = segments.getLastSegment().getLastEntry().orElse(null);
    }

    public IndexedLogEntry getLastEntry() {
        return lastLogEntry;
    }

    public synchronized IndexedLogEntry append(LogEntry logEntry) {
        try {
            lastLogEntry = segmentWriter.appendEntry(logEntry);
        } catch (BufferOverflowException e) {
            segmentWriter.release();
            Segment segment = segments.createNextSegment();
            segmentWriter = new SegmentWriter(segment);
            lastLogEntry = segmentWriter.appendEntry(logEntry);
        }
        return lastLogEntry;
    }

    public void append(Iterable<LogEntry> logEntries) {
        logEntries.forEach(this::append);
    }

    public synchronized IndexedLogEntry getEntryByIndex(long index) {
        Segment segment = segments.getSegment(index);
        return segment.getEntryByIndex(index);
    }

    public int getTermByIndex(long index) {
        return index <= 0 ? 0: getEntryByIndex(index).getLogEntry().getTerm();
    }

    public LogStorageReader openReader() {
        return openReader(1, () -> Long.MAX_VALUE);
    }

    public LogStorageReader openReader(long index) {
        return openReader(index, () -> Long.MAX_VALUE);
    }

    public LogStorageReader openReader(Supplier<Long> currentMaxIndexSupplier) {
        return openReader(1, currentMaxIndexSupplier);
    }

    public LogStorageReader openReader(long index, Supplier<Long> currentMaxIndexSupplier) {
        LogStorageReader reader = new ChunkLogStorageReader(segments, index, currentMaxIndexSupplier);
        logReaders.add(reader);
        return reader;
    }

    @Override
    public void close() {
        segments.release();
        segmentWriter.release();
        logReaders.forEach(LogStorageReader::close);
    }

    public synchronized void truncateFromIndex(long index) {
        long lastLogEntryIndex = segmentWriter.getLastLogEntryIndex();
        if (index > lastLogEntryIndex) {
            return;
        }

        Segment lastSegment = this.segments.getLastSegment();
        if (index < lastSegment.getFirstIndex()) {
            segmentWriter.release();
            lastSegment = segments.deleteSegments(index);
            segmentWriter = new SegmentWriter(lastSegment);
        }
        logReaders.forEach(logStorageReader -> {
            if (logStorageReader.getCurrentIndex() >= index) {
                // TODO thread safe (reset, hasNext, next)
                 logStorageReader.reset(index - 1);
            }
        });
        lastLogEntryIndex = segmentWriter.truncateFromIndex(index);
        lastLogEntry = lastSegment.getEntryByIndex(lastLogEntryIndex);
    }
}
