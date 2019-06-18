package io.github.pmackowski.rsocket.raft.storage.log;

import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.StorageException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NavigableMap;

import static org.assertj.core.api.Assertions.assertThat;

class SegmentsReaderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentsReaderTest.class);

    @TempDir
    Path directory;

    SegmentsReader segmentsReader = new SegmentsReader();

    RaftStorageConfiguration raftStorageConfiguration;

    @AfterEach
    void tearDown() {

    }

    @BeforeEach
    void setUp() {
        LOGGER.info("directory {}", directory);
        raftStorageConfiguration = RaftStorageConfiguration.builder()
                .directory(directory)
                .build();
    }

    @Test
    void readSegments() {
        createSegment(1, 1, 19, 100);
        createSegment(2, 20, 32, 200);

        final NavigableMap<Long, Segment> actual = segmentsReader.read(raftStorageConfiguration);
        assertThat(actual).hasSize(2);
        assertThat(actual.keySet()).containsExactly(1L, 20L);

        final Segment firstSegment = actual.get(1L);
        assertThat(firstSegment.getSegmentId()).isEqualTo(SegmentId.of(1));
        assertThat(firstSegment.getFirstIndex()).isEqualTo(1);
        assertThat(firstSegment.getSegmentHeader().getSegmentSize()).isEqualTo(100);

        final Segment secondSegment = actual.get(20L);
        assertThat(secondSegment.getSegmentId()).isEqualTo(SegmentId.of(2));
        assertThat(secondSegment.getFirstIndex()).isEqualTo(20);
        assertThat(secondSegment.getSegmentHeader().getSegmentSize()).isEqualTo(200);

        actual.entrySet().forEach(entry -> entry.getValue().release());
    }

    void createSegment(int segmentId, long firstIndex, long lastIndex, int segmentSize) {
        RandomAccessFile raf, rafIndex;
        FileChannel channel, channelIndex;
        try {
            raf = new RandomAccessFile(Paths.get(directory.toAbsolutePath().toString(), String.format(Segment.SEGMENT_FILE, segmentId)).toFile(), "rw");
            raf.setLength(segmentSize);
            channel = raf.getChannel();

            rafIndex = new RandomAccessFile(Paths.get(directory.toAbsolutePath().toString(), String.format(Segment.SEGMENT_INDEX_FILE, segmentId)).toFile(), "rw");
            rafIndex.setLength(segmentSize);
            channelIndex = rafIndex.getChannel();

        } catch (IOException e) {
            throw new StorageException(e);
        }

        try {
            ByteBuffer buffer = ByteBuffer.allocate(SegmentHeader.SIZE);
            buffer.putInt(segmentId);
            buffer.putLong(firstIndex);
            buffer.putInt(segmentSize);
            buffer.flip();
            channel.write(buffer);

            for (long j = firstIndex; j <= lastIndex; j++) {
                ByteBuffer intBuffer = ByteBuffer.allocate(Integer.SIZE);
                intBuffer.putInt(20);
                intBuffer.flip();
                channelIndex.write(intBuffer);
            }
        } catch (IOException e) {
            throw new StorageException(e);
        } finally {
            try {
                channel.close();
                raf.close();
                rafIndex.close();
            } catch (IOException e) {
                LOGGER.error("sss", e);
            }
        }
    }
}