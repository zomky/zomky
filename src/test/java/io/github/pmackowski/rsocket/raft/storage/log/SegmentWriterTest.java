package io.github.pmackowski.rsocket.raft.storage.log;

import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.github.pmackowski.rsocket.raft.storage.RaftStorageUtils.openChannel;
import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;
import static org.assertj.core.api.Assertions.assertThat;

class SegmentWriterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentWriterTest.class);

    @TempDir
    Path directory;

    SegmentWriter segmentWriter;
    Segments segments;
    Segment segment;

    @BeforeEach
    void setUp() {
        LOGGER.info("directory {}", directory);
        segments = new Segments(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.kilobytes, 8)
                .build()
        );
        segment = segments.getLastSegment();

    }

    @AfterEach
    void tearDown() {
        segmentWriter.release();
        segments.release();
    }

    @Test
    void append() throws Exception {
        // given
        segmentWriter = new SegmentWriter(segment);

        // when
        IntStream.rangeClosed(1, 10).forEach(i -> {
            CommandEntry commandEntry = new CommandEntry(1, i, ("abc"+i).getBytes());
            segmentWriter.appendEntry(commandEntry);
        });

        // then
        try(FileChannel channel = openChannel(segment.getSegmentPath())) {
            long fileSize = channel.size();
            ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
            channel.read(buffer);
            buffer.flip();

            buffer.position(SegmentHeader.SIZE);
            int i = 0;
            while (i < 10) {
                i++;
                int size = buffer.getInt();
                CommandEntry command = deserialize(buffer, size);
                assertThat(command.getValue()).isEqualTo(("abc"+i).getBytes());
                assertThat(command.getTerm()).isEqualTo(1);
            }
        }
    }

    @Test
    void appendToExistingSegment() {
        segmentWriter = new SegmentWriter(segment);
        CommandEntry commandEntry = new CommandEntry(1, 10, ("abc1").getBytes());
        IndexedLogEntry actual = segmentWriter.appendEntry(commandEntry);

        assertThat(actual.getIndex()).isEqualTo(1);
        assertThat(actual.getSize()).isEqualTo(Integer.BYTES + Long.BYTES + "abc1".length()); // term + timestamp
        CommandEntry actualCommandEntry = (CommandEntry) actual.getLogEntry();
        assertThat(actualCommandEntry.getTerm()).isEqualTo(1);
        assertThat(actualCommandEntry.getTimestamp()).isEqualTo(10);
        assertThat(actualCommandEntry.getValue()).isEqualTo("abc1".getBytes());
        assertThat(segmentWriter.getLastLogEntryIndex()).isEqualTo(1);

        segmentWriter.release();

        segmentWriter = new SegmentWriter(segment);
        commandEntry = new CommandEntry(2, 20, ("abc2").getBytes());
        actual = segmentWriter.appendEntry(commandEntry);
        assertThat(actual.getIndex()).isEqualTo(2);
        assertThat(actual.getSize()).isEqualTo(Integer.BYTES + Long.BYTES + "abc2".length()); // term + timestamp
        actualCommandEntry = (CommandEntry) actual.getLogEntry();
        assertThat(actualCommandEntry.getTerm()).isEqualTo(2);
        assertThat(actualCommandEntry.getTimestamp()).isEqualTo(20);
        assertThat(actualCommandEntry.getValue()).isEqualTo("abc2".getBytes());
        assertThat(segmentWriter.getLastLogEntryIndex()).isEqualTo(2);
    }


    @Test
    void truncate() {
        // given
        segmentWriter = new SegmentWriter(segment);

        IntStream.rangeClosed(1, 10).forEach(i -> {
            CommandEntry commandEntry = new CommandEntry(1, i, ("abc"+i).getBytes());
            segmentWriter.appendEntry(commandEntry);
        });

        // when
        long actual = segmentWriter.truncateFromIndex(5);

        // then
        assertThat(actual).isEqualTo(4);

    }

    @Test
    void truncateFromIndexGreaterThanLastIndex() {
        // given
        segmentWriter = new SegmentWriter(segment);

        IntStream.rangeClosed(1, 10).forEach(i -> {
            CommandEntry commandEntry = new CommandEntry(1, i, ("abc"+i).getBytes());
            segmentWriter.appendEntry(commandEntry);
        });

        // when
        long actual = segmentWriter.truncateFromIndex(50);

        // then
        assertThat(actual).isEqualTo(10);

    }
}