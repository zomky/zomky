package io.github.pmackowski.rsocket.raft.storage.log.entry;

import java.util.Objects;

public class IndexedLogEntry {

    private LogEntry logEntry;
    private long index;
    private int size;

    public IndexedLogEntry(LogEntry logEntry, long index, int size) {
        this.logEntry = logEntry;
        this.index = index;
        this.size = size;
    }

    public LogEntry getLogEntry() {
        return logEntry;
    }

    public long getIndex() {
        return index;
    }

    public int getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexedLogEntry that = (IndexedLogEntry) o;
        return index == that.index &&
                size == that.size &&
                logEntry.equals(that.logEntry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logEntry, index, size);
    }

    @Override
    public String toString() {
        return "IndexedLogEntry{" +
                "logEntry=" + logEntry +
                ", index=" + index +
                ", size=" + size +
                '}';
    }
}
