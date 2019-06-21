package io.github.pmackowski.rsocket.raft.storage.log.entry;

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
    public String toString() {
        return "IndexedLogEntry{" +
                "logEntry=" + logEntry +
                ", index=" + index +
                ", size=" + size +
                '}';
    }
}
