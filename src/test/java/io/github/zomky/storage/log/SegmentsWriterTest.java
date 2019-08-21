package io.github.zomky.storage.log;

import io.github.zomky.storage.StorageException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.github.zomky.storage.RaftStorageUtils.openChannel;
import static org.assertj.core.api.Assertions.assertThat;

class SegmentsWriterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentsWriterTest.class);

    @TempDir
    Path directory;

    SegmentsWriter segmentsWriter = new SegmentsWriter();
    Segment segment = null;

    @BeforeEach
    void setUp() {
        LOGGER.info("directory {}", directory);
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(segment.getSegmentPath());
        Files.deleteIfExists(segment.getSegmentIndexPath());
        segment.release();
    }

    @Test
    void createOrUpdateSegment() {
        // given
        int segmentId = 5;
        long firstIndex = 10;
        int segmentSize = 100;
        SegmentHeader segmentHeader = SegmentHeader.builder()
            .id(segmentId)
            .firstIndex(firstIndex)
            .segmentSize(segmentSize)
            .build();

        segment = new Segment(directory, segmentHeader);

        // when
        segmentsWriter.createOrUpdate(segment);

        // then
        assertSegmentFileExists(segmentId);
        assertSegmentIndexFileExists(segmentId);
        assertSegmentId(segmentId);
        assertFirstIndex(segmentId, firstIndex);
        assertSegmentSize(segmentId, segmentSize);
    }

    @Test
    void overwriteSegmentIfAlreadyExists() {
        // given
        int segmentId = 5;
        long firstIndex = 10;
        int segmentSize = 100;
        SegmentHeader segmentHeader = SegmentHeader.builder()
                .id(segmentId)
                .firstIndex(firstIndex)
                .segmentSize(segmentSize)
                .build();

        segment = new Segment(directory, segmentHeader);
        segmentsWriter.createOrUpdate(segment);

        final int nextSegmentId = segmentId + 1;
        segmentHeader = SegmentHeader.builder()
                .id(nextSegmentId)
                .firstIndex(firstIndex + firstIndex)
                .segmentSize(segmentSize + segmentSize)
                .build();
        segment.release();

        segment = new Segment(directory, segmentHeader);

        // when
        segmentsWriter.createOrUpdate(segment);

        // then
        assertSegmentFileExists(nextSegmentId);
        assertSegmentIndexFileExists(nextSegmentId);
        assertSegmentId(nextSegmentId);
        assertFirstIndex(nextSegmentId, firstIndex + firstIndex);
        assertSegmentSize(nextSegmentId, segmentSize + segmentSize);
    }

    void assertSegmentFileExists(int segmentId) {
        final Path path = segmentPath(segmentId);
        assertThat(Files.exists(path)).isTrue();
        assertThat(Files.isRegularFile(path)).isTrue();
    }

    void assertSegmentIndexFileExists(int segmentId) {
        final Path path = segmentIndexPath(segmentId);
        assertThat(Files.exists(path)).isTrue();
        assertThat(Files.isRegularFile(path)).isTrue();
    }

    void assertSegmentId(int segmentId) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
        try (FileChannel channel = openChannel(segmentPath(segmentId))) {
            channel.read(buffer);
            buffer.flip();
            final int id = buffer.getInt();
            assertThat(id).isEqualTo(segmentId);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    void assertFirstIndex(int segmentId, long firstIndex) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
        try (FileChannel channel = openChannel(segmentPath(segmentId))) {
            channel.read(buffer, SegmentHeader.SEGMENT_FIRST_INDEX_POSITION);
            buffer.flip();
            final long actualFirstIndex = buffer.getLong();
            assertThat(actualFirstIndex).isEqualTo(firstIndex);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    void assertSegmentSize(int segmentId, int segmentSize) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
        try (FileChannel channel = openChannel(segmentPath(segmentId))) {
            channel.read(buffer, SegmentHeader.SEGMENT_SEGMENT_SIZE_POSITION);
            buffer.flip();
            final long actualSegmentSize = buffer.getInt();
            assertThat(actualSegmentSize).isEqualTo(segmentSize);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    private Path segmentPath(int segmentId) {
        return Paths.get(directory.toAbsolutePath().toString(), String.format(Segment.SEGMENT_FILE, segmentId));
    }

    private Path segmentIndexPath(int segmentId) {
        return Paths.get(directory.toAbsolutePath().toString(), String.format(Segment.SEGMENT_INDEX_FILE, segmentId));
    }
}