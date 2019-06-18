package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.listener.ConfirmListener;
import io.github.pmackowski.rsocket.raft.storage.log.LogStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.meta.MetaStorage;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;

public class RaftStorage {

    private MetaStorage metaStorage;
    private LogStorage logStorage;
    private List<ConfirmListener> confirmListeners = new ArrayList<>();
    private long commitIndex;

    public RaftStorage(RaftStorageConfiguration configuration) {
        initialize(configuration);
        this.metaStorage = new MetaStorage(configuration);
        this.logStorage = new LogStorage(configuration);
    }

    public void commit(long commitIndex) {
        this.commitIndex = commitIndex;
        confirmListeners.forEach(listener -> listener.handle(commitIndex));
    }

    public void addConfirmListener(ConfirmListener confirmListener) {
        confirmListeners.add(confirmListener);
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

    public LogStorageReader openReader(long index) {
        return logStorage.openReader(index);
    }

    public LogStorageReader openReader() {
        return logStorage.openReader(1);
    }

    public void truncateFromIndex(long index) {
        throw new NotImplementedException();
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
