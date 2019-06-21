package io.github.pmackowski.rsocket.raft.storage.log.reader;

import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.serialize;

// TODO draft implementation
public class BoundedLogStorageReader {

    private LogStorageReader logStorageReader;

    public BoundedLogStorageReader(LogStorageReader logStorageReader) {
        this.logStorageReader = logStorageReader;
    }

    public List<ByteBuffer> getRawEntriesByIndex(long indexFrom, long indexTo) {
        List<ByteBuffer> result = new ArrayList<>();
        logStorageReader.reset(indexFrom);
        long i = indexTo - indexFrom + 1;
        while (logStorageReader.hasNext() && i > 0) {
            IndexedLogEntry entry = logStorageReader.next();
            ByteBuffer byteBuffer = ByteBuffer.allocate(entry.getSize());
            serialize(entry.getLogEntry(), byteBuffer);
            byteBuffer.flip();
            result.add(byteBuffer);
            i--;
        }
        return result;
    }

    public List<IndexedLogEntry> getEntriesByIndex(long indexFrom, long indexTo) {
        List<IndexedLogEntry> result = new ArrayList<>();
        logStorageReader.reset(indexFrom);
        long i = indexTo - indexFrom + 1;
        while (logStorageReader.hasNext() && i > 0) {
            IndexedLogEntry entry = logStorageReader.next();
            result.add(entry);
            i--;
        }
        return result;
    }

    public void close() {
        logStorageReader.close();
    }


}
