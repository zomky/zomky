package io.github.pmackowski.rsocket.raft.storage.log.reader;

import io.github.pmackowski.rsocket.raft.storage.BufferCleaner;
import io.github.pmackowski.rsocket.raft.storage.StorageException;
import io.github.pmackowski.rsocket.raft.storage.log.Segment;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.github.pmackowski.rsocket.raft.storage.RaftStorageUtils.openChannel;
import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;

// TODO
public class ChunkMmapSegmentReader implements SegmentReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkMmapSegmentReader.class);

    private Segment segment;
    private FileChannel segmentChannel;
    private FileChannel segmentIndexChannel;
    private ByteBuffer segmentBuffer;
    int nextPosition;

    private Long currentIndex = 1L;

    private final int chunkSize;

    public ChunkMmapSegmentReader(Segment segment, int chunkSize) {
        this(segment, segment.getFirstIndex(), chunkSize);
    }

    public ChunkMmapSegmentReader(Segment segment, long index, int chunkSize) {
        LOGGER.info("reader start");
        this.chunkSize = chunkSize;
        this.segment = segment;
        this.segmentChannel = openChannel(segment.getSegmentPath());
        this.segmentIndexChannel = openChannel(segment.getSegmentIndexPath());
        try {
            ByteBuffer segmentIndexBuffer = ByteBuffer.allocate(Integer.BYTES);
            segmentIndexChannel.read(segmentIndexBuffer, (index - segment.getFirstIndex()) * Integer.BYTES);
            segmentIndexBuffer.flip();
            int position = segmentIndexBuffer.getInt();
            segmentBuffer = segmentChannel.map(FileChannel.MapMode.READ_ONLY, position, chunkSize);
            nextPosition = position + chunkSize;
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public IndexedLogEntry next() {
        try {
            int entrySize = Integer.MAX_VALUE;
            int remaining = segmentBuffer.remaining();
            if (remaining >= Integer.BYTES) {
                entrySize = segmentBuffer.getInt();
            }
            if (entrySize > remaining - Integer.BYTES) {
                nextPosition = nextPosition - remaining;
                int currentChunkSize = chunkSize + remaining;
                BufferCleaner.closeDirectBuffer(segmentBuffer);
                segmentBuffer = segmentChannel.map(FileChannel.MapMode.READ_ONLY, nextPosition, currentChunkSize);
                entrySize = segmentBuffer.getInt();
                if (entrySize > currentChunkSize - Integer.BYTES) { // entry bigger than chunk size
                    BufferCleaner.closeDirectBuffer(segmentBuffer);
                    segmentBuffer = segmentChannel.map(FileChannel.MapMode.READ_ONLY, nextPosition + Integer.BYTES, entrySize);
                    nextPosition = nextPosition + Integer.BYTES + entrySize;
                } else {
                    nextPosition = nextPosition + currentChunkSize;
                }
            }

            LogEntry logEntry = deserialize(segmentBuffer, entrySize);
            return new IndexedLogEntry(logEntry, currentIndex++, entrySize);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reset(long index) {

    }

    @Override
    public long getCurrentIndex() {
        return 0;
    }

    @Override
    public void close()  {
        try {
            BufferCleaner.closeDirectBuffer(segmentBuffer);
            segmentChannel.close();
            segmentIndexChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
