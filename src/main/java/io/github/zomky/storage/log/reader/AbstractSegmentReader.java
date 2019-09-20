package io.github.zomky.storage.log.reader;

import io.github.zomky.storage.log.entry.IndexedLogEntry;

import java.util.NoSuchElementException;

public abstract class AbstractSegmentReader implements SegmentReader {

    protected IndexedLogEntry current;
    protected IndexedLogEntry next;

    @Override
    public boolean hasNext() {
        if (next == null) {
            readNext();
        }
        return next != null;
    }

    @Override
    public IndexedLogEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        current = next;
        next = null;
        readNext();
        return current;
    }

    protected abstract void readNext();

}
