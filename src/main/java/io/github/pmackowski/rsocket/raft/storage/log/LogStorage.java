package io.github.pmackowski.rsocket.raft.storage.log;

import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferOverflowException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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

    public IndexedLogEntry append(LogEntry logEntry) {
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

    public IndexedLogEntry getEntryByIndex(long index) {
        Segment segment = segments.getSegment(index);
        return segment.getEntryByIndex(index);
    }

    public int getTermByIndex(long index) {
        return index <= 0 ? 0: getEntryByIndex(index).getLogEntry().getTerm();
    }

    public LogStorageReader openReader(long index) {
        LogStorageReader reader = new LogStorageReader(segments, index);
        logReaders.add(reader);
        return reader;
    }

    @Override
    public void close() {
        segments.release();
        segmentWriter.release();
        logReaders.forEach(LogStorageReader::close);
    }
}
