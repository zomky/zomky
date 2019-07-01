package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;

public class InMemoryRaftStorage implements RaftStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryRaftStorage.class);

    private List<IndexedLogEntry> entries = new CopyOnWriteArrayList<>();
    private IndexedLogEntry last;
    private volatile long commitIndex;
    private volatile int term;
    private volatile int votedFor;

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
        int size = 0;
        if (logEntry instanceof CommandEntry) {
            CommandEntry commandEntry = (CommandEntry) logEntry;
            size = commandEntry.getValue().length + LogEntry.SIZE +1;
        }
        last = new IndexedLogEntry(logEntry, entries.size() + 1, size);
        entries.add(last);
        return last;
    }

    @Override
    public void truncateFromIndex(long index) {
        LOGGER.info("truncate index {}", index);
        if (index > entries.size()) {
            LOGGER.info("truncate index greater than entries size {}, entries size {}", index, entries.size());
        } else if (index > 0 && index <= entries.size()) {
            entries.subList((int) index - 1, entries.size()).clear();
            if (entries.size() > 0) {
                last = entries.get(entries.size() - 1);
            } else {
                last = null;
            }
        }

    }

    @Override
    public Optional<IndexedLogEntry> getLastEntry() {
        return Optional.ofNullable(last);
    }

    @Override
    public Optional<IndexedLogEntry> getEntryByIndex(long index) {
        if (entries.size() == 0 || index == 0 || index > entries.size()) {
            return Optional.empty();
        }
        return Optional.of(entries.get((int) index - 1));
    }

    @Override
    public int getTermByIndex(long index) {
        if (entries.size() == 0) {
            return 0;
        }
        return getEntryByIndex(index)
                .map(IndexedLogEntry::getLogEntry)
                .map(LogEntry::getTerm)
                .orElse(0);
    }

    @Override
    public LogStorageReader openReader() {
        return new InMemoryLogStorageReader(entries, () -> Long.MAX_VALUE);
    }

    @Override
    public LogStorageReader openReader(long index) {
        return new InMemoryLogStorageReader(entries, index, () -> Long.MAX_VALUE);
    }

    @Override
    public LogStorageReader openCommittedEntriesReader() {
        return new InMemoryLogStorageReader(entries, () -> commitIndex);
    }

    @Override
    public LogStorageReader openCommittedEntriesReader(long index) {
        return new InMemoryLogStorageReader(entries, index, () -> commitIndex);
    }

    @Override
    public void close() {

    }

    @Override
    public void updateConfiguration(Configuration configuration) {
        throw new NotImplementedException();
    }

    @Override
    public Configuration getConfiguration() {
        throw new NotImplementedException();
    }

    private static class InMemoryLogStorageReader implements LogStorageReader {

        private List<IndexedLogEntry> entries;
        private Iterator<IndexedLogEntry> iterator;
        private long initialIndex;
        private long currentIndex;
        private Supplier<Long> currentMaxIndexSupplier;

        public InMemoryLogStorageReader(List<IndexedLogEntry> entries, Supplier<Long> currentMaxIndexSupplier) {
            this(entries, 1L, currentMaxIndexSupplier);
        }

        public InMemoryLogStorageReader(List<IndexedLogEntry> entries, long index, Supplier<Long> currentMaxIndexSupplier) {
            this.entries = entries;
            this.initialIndex = index;
            this.currentIndex = index;
            this.currentMaxIndexSupplier = currentMaxIndexSupplier;
            reset(index);
        }

        @Override
        public void reset() {
            iterator = entries.listIterator((int) initialIndex - 1);
            currentIndex = initialIndex;
        }

        @Override
        public void reset(long index) {
            iterator = entries.listIterator((int) index - 1);
            currentIndex = index;
        }

        @Override
        public void close() {

        }

        @Override
        public long getCurrentIndex() {
            return currentIndex;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext() && currentMaxIndexSupplier.get() >= currentIndex;
        }

        @Override
        public IndexedLogEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            currentIndex++;
            return iterator.next();
        }
    }
}
