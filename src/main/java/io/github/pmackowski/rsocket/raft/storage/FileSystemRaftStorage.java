package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.LogStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import io.github.pmackowski.rsocket.raft.storage.meta.MetaStorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Optional;

import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;

public class FileSystemRaftStorage implements RaftStorage {

    private MetaStorage metaStorage;
    private LogStorage logStorage;

    /**
     * index of highest log entry known to be
     * committed (initialized to 0, increases
     * monotonically)
     */
    private volatile long commitIndex;

    public FileSystemRaftStorage(RaftStorageConfiguration configuration) {
        initialize(configuration);
        this.metaStorage = new MetaStorage(configuration);
        this.logStorage = new LogStorage(configuration);
    }

    public void commit(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public int getTerm() {
        return metaStorage.getTerm();
    }

    public int getVotedFor() {
        return metaStorage.getVotedFor();
    }

    public void update(int term, int votedFor) {
        metaStorage.update(term, votedFor);
    }

    public IndexedLogEntry append(ByteBuffer logEntry) {
        return logStorage.append(deserialize(logEntry));
    }

    public IndexedLogEntry append(LogEntry logEntry) {
        return logStorage.append(logEntry);
    }

    public LogStorageReader openReader() {
        return logStorage.openReader();
    }

    public LogStorageReader openReader(long index) {
        return logStorage.openReader(index);
    }

    public LogStorageReader openCommitedEntriesReader() {
        return logStorage.openReader(() -> this.commitIndex);
    }

    public LogStorageReader openCommitedEntriesReader(long index) {
        return logStorage.openReader(index, () -> this.commitIndex);
    }

    public void truncateFromIndex(long index) {
        logStorage.truncateFromIndex(index);
    }

    public IndexedLogEntry getLast() { // TODO
        return Optional.ofNullable(logStorage.getLastEntry()).orElse(new IndexedLogEntry(new CommandEntry(0,0, "".getBytes()), 0, 0));
    }

    public IndexedLogEntry getEntryByIndex(long index) {
        return logStorage.getEntryByIndex(index);
    }

    public int getTermByIndex(long index) {
        return logStorage.getTermByIndex(index);
    }

    private void initialize(RaftStorageConfiguration configuration) {
        try {
            if (Files.notExists(configuration.getDirectory())) {
                Files.createDirectory(configuration.getDirectory());
            }
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public void close() {
        logStorage.close();
        metaStorage.close();
    }
}
