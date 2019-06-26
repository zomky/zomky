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

import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class SegmentTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentTest.class);

    @TempDir
    Path directory;

    Segments segments;
    Segment firstSegment, nextSegment;
    SegmentWriter firstSegmentWriter, nextSegmentWriter;

    @BeforeEach
    void setUp() {
        LOGGER.info("directory {}", directory);
        segments = new Segments(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.kilobytes, 4)
                .build()
        );

        firstSegment = segments.getLastSegment();
        firstSegmentWriter = new SegmentWriter(firstSegment);
    }

    @AfterEach
    void tearDown() {
        segments.release();
        firstSegmentWriter.release();
        if (nextSegmentWriter != null) {
            nextSegmentWriter.release();
        }
    }

    @Test
    void firstSegmentNoEntries() {
        assertThat(firstSegment.getFirstIndex()).isEqualTo(1);
        assertThat(firstSegment.getLastIndex()).isEqualTo(0);

        assertThat(firstSegment.getEntryByIndex(1)).isEmpty();
    }

    @Test
    void firstSegment() {
        int firstIndex = 1;
        int entries = 100;
        long timestamp = System.currentTimeMillis();

        IntStream.rangeClosed(1, entries).forEach(i -> {
            firstSegmentWriter.appendEntry(commandEntry(i, i, timestamp + i));
        });

        assertThat(firstSegment.getFirstIndex()).isEqualTo(firstIndex);
        assertThat(firstSegment.getLastIndex()).isEqualTo(entries);
        assertIndexLogEntry(firstSegment.getEntryByIndex(firstIndex), commandEntry(firstIndex, firstIndex, timestamp + firstIndex), firstIndex, 16);
        assertIndexLogEntry(firstSegment.getEntryByIndex(10), commandEntry(10, 10, timestamp + 10), 10, 17);
        assertIndexLogEntry(firstSegment.getEntryByIndex(entries), commandEntry(entries, entries, timestamp + entries), entries, 18);
    }

    @Test
    void nextSegment() {
        int firstIndex = 32;
        int entries = 100;
        int lastIndex = firstIndex + entries - 1;
        long timestamp = System.currentTimeMillis();
        nextSegment = segments.createNextSegment(firstIndex);
        nextSegmentWriter = new SegmentWriter(nextSegment);
        IntStream.rangeClosed(1, entries).forEach(i -> {
            nextSegmentWriter.appendEntry(commandEntry(i, i, timestamp + i));
        });

        assertThat(nextSegment.getFirstIndex()).isEqualTo(firstIndex);
        assertThat(nextSegment.getLastIndex()).isEqualTo(lastIndex);
        assertIndexLogEntry(nextSegment.getEntryByIndex(firstIndex), commandEntry(1, 1, timestamp + 1), firstIndex, 16);
        assertIndexLogEntry(nextSegment.getEntryByIndex(41), commandEntry(10, 10, timestamp + 10), 41, 17);
        assertIndexLogEntry(nextSegment.getEntryByIndex(lastIndex), commandEntry(entries, entries, timestamp + entries), lastIndex, 18);
    }

    private void assertIndexLogEntry(Optional<IndexedLogEntry> optActual, CommandEntry expectedEntry, long expectedIndex, int expectedSize) {
        assertThat(optActual).isNotEmpty();
        IndexedLogEntry actual = optActual.get();
        assertIndexLogEntry(actual, expectedEntry, expectedIndex, expectedSize);
    }

    private void assertIndexLogEntry(IndexedLogEntry actual, CommandEntry expectedEntry, long expectedIndex, int expectedSize) {
        assertThat(actual.getIndex()).isEqualTo(expectedIndex);
        assertThat(actual.getSize()).isEqualTo(expectedSize);
        CommandEntry actualLogEntry = (CommandEntry) actual.getLogEntry();
        assertThat(actualLogEntry.getTerm()).isEqualTo(expectedEntry.getTerm());
        assertThat(actualLogEntry.getTimestamp()).isEqualTo(expectedEntry.getTimestamp());
        assertThat(actualLogEntry.getValue()).isEqualTo(expectedEntry.getValue());
    }

    private CommandEntry commandEntry(int i, int term, long timestamp) {
        return new CommandEntry(term, timestamp, ("abc" + i).getBytes());
    }
}