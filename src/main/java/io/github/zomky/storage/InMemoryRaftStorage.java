package io.github.zomky.storage;

import io.github.zomky.storage.log.entry.CommandEntry;
import io.github.zomky.storage.log.entry.ConfigurationEntry;
import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.LogEntry;
import io.github.zomky.storage.log.reader.LogStorageReader;
import io.github.zomky.storage.log.serializer.LogEntrySerializer;
import io.github.zomky.storage.meta.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Supplier;

public class InMemoryRaftStorage implements RaftStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryRaftStorage.class);

    private List<IndexedLogEntry> entries = Collections.synchronizedList(new ArrayList<>());

    private IndexedLogEntry last;
    private volatile int term;
    private volatile int votedFor;
    private Configuration configuration;

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
        return append(LogEntrySerializer.deserialize(logEntry));
    }

    @Override
    public IndexedLogEntry append(LogEntry logEntry) {
        int size = LogEntry.SIZE +1;
        if (logEntry instanceof CommandEntry) {
            CommandEntry commandEntry = (CommandEntry) logEntry;
            size = size + commandEntry.getValue().length;
        }
        if (logEntry instanceof ConfigurationEntry) {
            ConfigurationEntry configurationEntry = (ConfigurationEntry) logEntry;
            size = size + configurationEntry.getMembers().size() * Integer.BYTES;
        }
        last = new IndexedLogEntry(logEntry, entries.size() + 1, size);
        entries.add(last);
        return last;
    }

    @Override
    public void truncateFromIndex(long index) {
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
    public LogStorageReader openReader(Supplier<Long> maxIndexSupplier) {
        return new InMemoryLogStorageReader(entries, maxIndexSupplier);
    }

    @Override
    public void close() {

    }

    @Override
    public void updateConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    private static class InMemoryLogStorageReader implements LogStorageReader {

        private List<IndexedLogEntry> entries;
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
            currentIndex = initialIndex;
        }

        @Override
        public void reset(long index) {
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
            return entries.size() >= currentIndex && currentMaxIndexSupplier.get() >= currentIndex;
        }

        @Override
        public IndexedLogEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            IndexedLogEntry indexedLogEntry = entries.get((int) currentIndex - 1);
            currentIndex++;
            return indexedLogEntry;
        }
    }
}
