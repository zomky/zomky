package io.github.zomky.storage.log.reader;

import io.github.zomky.storage.log.entry.IndexedLogEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.github.zomky.storage.log.serializer.LogEntrySerializer.serialize;

public class BoundedLogStorageReader {

    private LogStorageReader logStorageReader;

    public BoundedLogStorageReader(LogStorageReader logStorageReader) {
        this.logStorageReader = logStorageReader;
    }

    public List<ByteBuffer> getRawEntriesByIndex(long indexFrom, long indexTo) {
        return getRawEntriesByIndex(indexFrom, indexTo, Integer.MAX_VALUE);
    }

    public List<ByteBuffer> getRawEntriesByIndex(long indexFrom, long indexTo, int maxBatchSize) {
        Function<IndexedLogEntry, ByteBuffer> mapEntry = entry -> {
            ByteBuffer byteBuffer = ByteBuffer.allocate(entry.getSize());
            serialize(entry.getLogEntry(), byteBuffer);
            byteBuffer.flip();
            return byteBuffer;
        };
        return getEntriesByIndex(indexFrom, indexTo, maxBatchSize, mapEntry);
    }

    public List<IndexedLogEntry> getEntriesByIndex(long indexFrom, long indexTo) {
        return getEntriesByIndex(indexFrom, indexTo, Integer.MAX_VALUE);
    }

    public List<IndexedLogEntry> getEntriesByIndex(long indexFrom, long indexTo, int maxBatchSize) {
        return getEntriesByIndex(indexFrom, indexTo, maxBatchSize, Function.identity());
    }

    private <T> List<T> getEntriesByIndex(long indexFrom, long indexTo, int maxBatchSize, Function<IndexedLogEntry, T> mapEntry) {
        int currentBatchSize = 0;
        List<T> result = new ArrayList<>();
        logStorageReader.reset(indexFrom);
        long i = indexTo - indexFrom + 1;
        long firstIteration = i;
        while (logStorageReader.hasNext() && i > 0) {
            IndexedLogEntry entry = logStorageReader.next();
            currentBatchSize = currentBatchSize + entry.getSize();
            if (currentBatchSize > maxBatchSize && i != firstIteration) {
                return result;
            }
            result.add(mapEntry.apply(entry));
            i--;
        }
        return result;
    }

    public void close() {
        logStorageReader.close();
    }

}
