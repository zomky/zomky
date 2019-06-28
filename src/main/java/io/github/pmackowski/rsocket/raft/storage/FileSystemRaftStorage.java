package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.LogStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
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

    @Override
    public void commit(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    @Override
    public int getTerm() {
        return metaStorage.getTerm();
    }

    @Override
    public int getVotedFor() {
        return metaStorage.getVotedFor();
    }

    @Override
    public void update(int term, int votedFor) {
        metaStorage.update(term, votedFor);
    }

    @Override
    public IndexedLogEntry append(ByteBuffer logEntry) {
        return logStorage.append(deserialize(logEntry));
    }

    @Override
    public IndexedLogEntry append(LogEntry logEntry) {
        return logStorage.append(logEntry);
    }

    @Override
    public LogStorageReader openReader() {
        return logStorage.openReader();
    }

    @Override
    public LogStorageReader openReader(long index) {
        return logStorage.openReader(index);
    }

    @Override
    public LogStorageReader openCommittedEntriesReader() {
        return logStorage.openReader(() -> this.commitIndex);
    }

    @Override
    public LogStorageReader openCommittedEntriesReader(long index) {
        return logStorage.openReader(index, () -> this.commitIndex);
    }

    @Override
    public void truncateFromIndex(long index) {
        logStorage.truncateFromIndex(index);
    }

    @Override
    public Optional<IndexedLogEntry> getLastEntry() {
        return logStorage.getLastEntry();
    }

    @Override
    public Optional<IndexedLogEntry> getEntryByIndex(long index) {
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

    @Override
    public void updateConfiguration(Configuration configuration) {
        metaStorage.updateConfiguration(configuration);
    }

    @Override
    public Configuration getConfiguration() {
        return metaStorage.getConfiguration();
    }
}
