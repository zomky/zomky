package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedTerm;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;

import java.nio.ByteBuffer;
import java.util.Optional;

public interface RaftStorage {

    void commit(long commitIndex);

    long commitIndex();

    int getTerm();

    int getVotedFor();

    void update(int term, int votedFor);

    IndexedLogEntry append(ByteBuffer logEntry);

    IndexedLogEntry append(LogEntry logEntry);

    LogStorageReader openReader();

    LogStorageReader openReader(long index);

    LogStorageReader openCommittedEntriesReader();

    LogStorageReader openCommittedEntriesReader(long index);

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

}
