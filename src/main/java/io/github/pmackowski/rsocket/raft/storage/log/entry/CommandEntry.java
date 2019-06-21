package io.github.pmackowski.rsocket.raft.storage.log.entry;

public class CommandEntry extends LogEntry {

    private byte[] value;

    public CommandEntry(int term, long timestamp, byte[] value) {
        super(term, timestamp);
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "CommandEntry{" +
                "value=" + new String(value) +
                ", term=" + term +
                ", timestamp=" + timestamp +
                '}';
    }
}
