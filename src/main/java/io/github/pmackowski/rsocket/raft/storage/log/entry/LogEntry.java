package io.github.pmackowski.rsocket.raft.storage.log.entry;

public class LogEntry {

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
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                ", timestamp=" + timestamp +
                '}';
    }
}
