package io.github.pmackowski.rsocket.raft.storage.log;

import io.github.pmackowski.rsocket.raft.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class SegmentsWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentsWriter.class);

    public void createOrUpdate(Segment segment) {
        SegmentHeader segmentHeader = segment.getSegmentHeader();
        Path segmentFile = segment.getSegmentPath();
        Path segmentIndexFile = segment.getSegmentIndexPath();

        RandomAccessFile raf, rafIndex;
        FileChannel channel;
        try {
            raf = new RandomAccessFile(segmentFile.toFile(), "rw");
            raf.setLength(segmentHeader.getSegmentSize());
            channel = raf.getChannel();

            rafIndex = new RandomAccessFile(segmentIndexFile.toFile(), "rw");
            rafIndex.setLength(segmentHeader.getSegmentIndexSize());
        } catch (IOException e) {
            throw new StorageException(e);
        }

        try {
            channel.write(segmentHeader.asBuffer());
        } catch (IOException e) {
            throw new StorageException(String.format("Segment header has not been written!"), e);
        } finally {
            try {
                channel.close();
                raf.close();
                rafIndex.close();
            } catch (IOException e) {
                LOGGER.warn("Segment after creation has not been released!", e);
            }
        }
    }
}
