package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;

public class InMemoryRaftStorage implements RaftStorage {

    private List<IndexedLogEntry> entries = new CopyOnWriteArrayList<>();
    private IndexedLogEntry last = new IndexedLogEntry(new CommandEntry(0, 0, "".getBytes()), 0, 0);
    private long commitIndex;
    private int term;
    private int votedFor;

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
        return term;
    }

    @Override
    public int getVotedFor() {
        return votedFor;
    }

    @Override
    public synchronized void update(int term, int votedFor) {
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override
    public IndexedLogEntry append(ByteBuffer logEntry) {
        return append(deserialize(logEntry));
    }

    @Override
    public IndexedLogEntry append(LogEntry logEntry) {
        last = new IndexedLogEntry(logEntry, entries.size() + 1, 0);
        entries.add(last);
        return last;
    }

    @Override
    public void truncateFromIndex(long index) {
        entries.subList((int) index - 1, entries.size()).clear();
    }

    @Override
    public IndexedLogEntry getLast() {
        return last;
    }

    @Override
    public IndexedLogEntry getEntryByIndex(long index) {
        if (entries.size() == 0) {
            return last;
        }
        return entries.get((int) index - 1);
    }

    @Override
    public int getTermByIndex(long index) {
        if (entries.size() == 0) {
            return 0;
        }
        return getEntryByIndex(index).getLogEntry().getTerm();
    }

    @Override
    public LogStorageReader openReader() {
        return new InMemoryLogStorageReader(entries);
    }

    @Override
    public LogStorageReader openReader(long index) {
        return new InMemoryLogStorageReader(entries, index);
    }

    @Override
    public LogStorageReader openCommitedEntriesReader() {
        throw new NotImplementedException();
    }

    @Override
    public LogStorageReader openCommitedEntriesReader(long index) {
        throw new NotImplementedException();
    }

    @Override
    public void close() {

    }
}
