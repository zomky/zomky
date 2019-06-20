package io.github.pmackowski.rsocket.raft.storage.log;

import io.github.pmackowski.rsocket.raft.storage.BufferCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.github.pmackowski.rsocket.raft.storage.RaftStorageUtils.mapReadWrite;
import static io.github.pmackowski.rsocket.raft.storage.RaftStorageUtils.openChannel;

public class SegmentIndexWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentIndexWriter.class);

    private FileChannel segmentIndexChannel;
    private MappedByteBuffer mappedSegmentIndexBuffer;

    public SegmentIndexWriter(Segment segment) {
        this.segmentIndexChannel = openChannel(segment.getSegmentIndexPath());
        this.mappedSegmentIndexBuffer = mapReadWrite(segmentIndexChannel, segment);
    }

    int position(int index) {
        int idx = Math.max(0, index - 1);
        return mappedSegmentIndexBuffer.getInt(idx * Integer.BYTES);
    }

    public void rewind(int entriesCount) {
        mappedSegmentIndexBuffer.position(entriesCount * Integer.BYTES);
    }

    public void append(int position) {
        mappedSegmentIndexBuffer.putInt(position);
    }

    public void release() {
        try {
            BufferCleaner.closeDirectBuffer(mappedSegmentIndexBuffer);
        } catch (Exception e) {
            LOGGER.error("Failed to unmap direct buffer", e);
        }
        try {
            segmentIndexChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void truncateFromIndex(int index) {
        rewind(index);
        try {
            mappedSegmentIndexBuffer.mark();
            while (mappedSegmentIndexBuffer.getInt() > 0) {
                mappedSegmentIndexBuffer.reset();
                mappedSegmentIndexBuffer.putInt(0);
                mappedSegmentIndexBuffer.mark();
            }
        } catch (BufferOverflowException e) {}
    }
}
