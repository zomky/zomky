package io.github.zomky.storage.log.serializer;

import io.github.zomky.storage.StorageException;
import io.github.zomky.storage.log.entry.CommandEntry;
import io.github.zomky.storage.log.entry.ConfigurationEntry;
import io.github.zomky.storage.log.entry.LogEntry;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

public class LogEntrySerializer {

    private static final byte COMMAND_ENTRY = 1;
    private static final byte CONFIGURATION_ENTRY = 2;

    private static final byte ENTRY_TYPE_SIZE = Byte.BYTES;

    public static int serialize(LogEntry logEntry, ByteBuffer buffer) {
        buffer.putInt(logEntry.getTerm());
        buffer.putLong(logEntry.getTimestamp());
        int size = 0;
        if (logEntry instanceof CommandEntry) {
            buffer.put(COMMAND_ENTRY);
            CommandEntry commandEntry = (CommandEntry) logEntry;
            buffer.put(commandEntry.getValue());
            size = commandEntry.getValue().length * Byte.BYTES + ENTRY_TYPE_SIZE;
        }
        if (logEntry instanceof ConfigurationEntry) {
            buffer.put(CONFIGURATION_ENTRY);
            ConfigurationEntry configurationEntry = (ConfigurationEntry) logEntry;
            configurationEntry.getMembers().forEach(buffer::putInt);
            size = configurationEntry.getMembers().size() * Integer.BYTES + ENTRY_TYPE_SIZE;
        }
        return LogEntry.SIZE + size;
    }

    public static <T extends LogEntry> T deserialize(ByteBuffer buffer, Class<T> type) {
        return type.cast(deserialize(buffer, buffer.remaining()));
    }

    public static LogEntry deserialize(ByteBuffer buffer) {
        return deserialize(buffer, buffer.remaining());
    }

    public static <T extends LogEntry> T deserialize(ByteBuffer buffer, int length, Class<T> type) {
        return type.cast(deserialize(buffer, length));
    }

    public static LogEntry deserialize(ByteBuffer buffer, int length) {
        int term = buffer.getInt();
        long timestamp = buffer.getLong();
        byte entryType = buffer.get();
        if (entryType == COMMAND_ENTRY) {
            byte[] value = new byte[length - LogEntry.SIZE - ENTRY_TYPE_SIZE];
            buffer.get(value);
            return new CommandEntry(term, timestamp, value);
        } else if (entryType == CONFIGURATION_ENTRY) {
            final int i = (length - LogEntry.SIZE - ENTRY_TYPE_SIZE) / Integer.BYTES;
            Set<Integer> members = new HashSet<>();
            IntStream.rangeClosed(1, i).forEach(j -> {
                members.add(buffer.getInt());
            });
            return new ConfigurationEntry(term, timestamp, members);
        } else {
            throw new StorageException("Cannot deserialize!!");
        }
    }

}
