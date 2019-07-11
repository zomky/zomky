package io.github.pmackowski.rsocket.raft.storage.log.entry;

import java.util.Objects;

public class LogEntry {

    public static final int SIZE = Integer.BYTES + Long.BYTES;

    protected int term;
    protected long timestamp;

    public LogEntry(int term, long timestamp) {
        this.term = term;
        this.timestamp = timestamp;
    }

    public int getTerm() {
        return term;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isCommand() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry logEntry = (LogEntry) o;
        return term == logEntry.term &&
                timestamp == logEntry.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, timestamp);
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                ", timestamp=" + timestamp +
                '}';
    }
}
