package io.github.zomky.storage.log.reader;

import io.github.zomky.storage.log.entry.IndexedLogEntry;

import java.util.Iterator;

public interface LogStorageReader extends Iterator<IndexedLogEntry>, AutoCloseable {

    void reset();

    void reset(long index);

    @Override
    void close();

    long getCurrentIndex();

}
