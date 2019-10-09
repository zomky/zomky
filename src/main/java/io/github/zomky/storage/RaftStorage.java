package io.github.zomky.storage;

import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.IndexedTerm;
import io.github.zomky.storage.log.entry.LogEntry;
import io.github.zomky.storage.log.reader.LogStorageReader;
import io.github.zomky.storage.meta.Configuration;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Supplier;

public interface RaftStorage {

    int getTerm();

    int getVotedFor();

    void update(int term, int votedFor);

    IndexedLogEntry append(ByteBuffer logEntry);

    IndexedLogEntry append(LogEntry logEntry);

    LogStorageReader openReader();

    LogStorageReader openReader(long index);

    LogStorageReader openReader(Supplier<Long> maxIndexSupplier);

    void truncateFromIndex(long index);

    default IndexedTerm getLastIndexedTerm() {
        return getLastEntry()
                .map(lastEntry -> new IndexedTerm(lastEntry.getIndex(), lastEntry.getLogEntry().getTerm()))
                .orElse(new IndexedTerm(0,0));
    }

    Optional<IndexedLogEntry> getLastEntry();

    Optional<IndexedLogEntry> getEntryByIndex(long index);

    int getTermByIndex(long index);

    void close();

    void updateConfiguration(Configuration configuration);

    Configuration getConfiguration();

}
