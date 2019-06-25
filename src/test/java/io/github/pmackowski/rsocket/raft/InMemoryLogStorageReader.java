package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;

import java.util.Iterator;
import java.util.List;

public class InMemoryLogStorageReader implements LogStorageReader {

    private List<IndexedLogEntry> entries;
    private Iterator<IndexedLogEntry> iterator;
    private long initialIndex;
    private long currentIndex;

    public InMemoryLogStorageReader(List<IndexedLogEntry> entries) {
        this.entries = entries;
        this.currentIndex = 1;
    }

    public InMemoryLogStorageReader(List<IndexedLogEntry> entries, long index) {
        this.entries = entries;
        this.initialIndex = index;
        this.currentIndex = index;
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
        return iterator.hasNext();
    }

    @Override
    public IndexedLogEntry next() {
        currentIndex++;
        return iterator.next();
    }
}
