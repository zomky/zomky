package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;

import java.nio.ByteBuffer;

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

    LogStorageReader openCommitedEntriesReader();

    LogStorageReader openCommitedEntriesReader(long index);

    void truncateFromIndex(long index);

    IndexedLogEntry getLast();

    IndexedLogEntry getEntryByIndex(long index);

    int getTermByIndex(long index);

    void close();

}
