package io.github.pmackowski.rsocket.raft.storage.log.serializer;

import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.ConfigurationEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;

import java.nio.ByteBuffer;

public class LogEntrySerializer {

    public static int serialize(LogEntry logEntry, ByteBuffer buffer) {
        buffer.putInt(logEntry.getTerm());
        buffer.putLong(logEntry.getTimestamp());
        int size = 0;
        if (logEntry instanceof CommandEntry) {
            CommandEntry commandEntry = (CommandEntry) logEntry;
            buffer.put(commandEntry.getValue());
            size = commandEntry.getValue().length * Byte.BYTES;
        }
        if (logEntry instanceof ConfigurationEntry) {
            ConfigurationEntry configurationEntry = (ConfigurationEntry) logEntry;
            configurationEntry.getMembers().forEach(buffer::putInt);
            size = configurationEntry.getMembers().size() * Integer.BYTES;
        }
        return Integer.BYTES + Long.BYTES + size;
    }

    public static CommandEntry deserialize(ByteBuffer buffer) {
        return deserialize(buffer, buffer.remaining());
    }

    public static CommandEntry deserialize(ByteBuffer buffer, int length) {
        int term = buffer.getInt();
        long timestamp = buffer.getLong();
        byte[] value = new byte[length - Integer.BYTES - Long.BYTES];
        buffer.get(value);
        return new CommandEntry(term, timestamp, value);
    }

}
