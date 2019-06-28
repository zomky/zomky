package io.github.pmackowski.rsocket.raft.storage.log;

import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class LogStorageTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogStorageTest.class);

    @TempDir
    Path directory;

    LogStorage logStorage;

    @AfterEach
    void tearDown() {
        logStorage.close();
    }

    @BeforeEach
    void setUp() {
        LOGGER.info("directory {}", directory);
    }

    @Test
    void append() {
        logStorage = new LogStorage(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.kilobytes, 8)
                .build());

        long timestamp = System.currentTimeMillis();
        int i = 1;
        String value = "abc" + i;

        final IndexedLogEntry actual = appendEntry(i, entry -> timestamp + entry, entry -> value);

        assertIndexLogEntry(actual, commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        assertIndexLogEntry(logStorage.getEntryByIndex(1), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        assertThat(logStorage.getTermByIndex(1)).isEqualTo(1);
        assertThat(logStorage.getLastEntry()).hasValue(actual);
    }

    @Test
    void appendToExistingSegment() {
        logStorage = new LogStorage(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.kilobytes, 8)
                .build());

        long timestamp = System.currentTimeMillis();

        IndexedLogEntry actual = appendEntry(1, entry -> timestamp + entry, entry -> "abc1");
        assertIndexLogEntry(actual, commandEntry(1, timestamp + 1, "abc1"), 1, LogEntry.SIZE + 1 + "abc1".length());
        logStorage.close();

        logStorage = new LogStorage(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.kilobytes, 8)
                .build());

        actual = appendEntry(2, entry -> timestamp + entry, entry -> "abc2");
        assertIndexLogEntry(actual, commandEntry(2, timestamp + 2, "abc2"), 2, LogEntry.SIZE + 1 + "abc2".length());

        assertIndexLogEntry(logStorage.getEntryByIndex(2), commandEntry(2, timestamp + 2, "abc2"), 2, LogEntry.SIZE + 1 + "abc2".length());
        assertThat(logStorage.getTermByIndex(2)).isEqualTo(2);
        assertThat(logStorage.getLastEntry()).hasValue(actual);
    }

    @Test
    void appendExceedsSegment() {
        logStorage = new LogStorage(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.bytes, 100)
                .build());

        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        IntStream.rangeClosed(1, numberOfEntries).forEach(i -> {
            String value = "abc" + i;
            assertIndexLogEntry(logStorage.getEntryByIndex(i), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
            assertThat(logStorage.getTermByIndex(i)).isEqualTo(i);
        });
        assertThat(logStorage.getLastEntry().get().getIndex()).isEqualTo(numberOfEntries);
    }

    @Test
    void truncate() {
        logStorage = new LogStorage(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.bytes, 1024)
                .build());

        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        logStorage.truncateFromIndex(5);

        IntStream.rangeClosed(1, 4).forEach(i -> {
            String value = "abc" + i;
            assertIndexLogEntry(logStorage.getEntryByIndex(i), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
            assertThat(logStorage.getTermByIndex(i)).isEqualTo(i);
        });
        assertThat(logStorage.getLastEntry().get().getIndex()).isEqualTo(4);
        assertThat(logStorage.getEntryByIndex(5)).isEmpty();
    }

    @Test
    void truncateManySegments() {
        logStorage = new LogStorage(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.bytes, 100)
                .build());

        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        logStorage.truncateFromIndex(5);

        IntStream.rangeClosed(1, 4).forEach(i -> {
            String value = "abc" + i;
            assertIndexLogEntry(logStorage.getEntryByIndex(i), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
            assertThat(logStorage.getTermByIndex(i)).isEqualTo(i);
        });
        assertThat(logStorage.getLastEntry().get().getIndex()).isEqualTo(4);
        assertThat(logStorage.getEntryByIndex(5)).isEmpty();
    }

    @Test
    void truncateWhenReaderIsOpened() {
        logStorage = new LogStorage(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.bytes, 1024)
                .build());

        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        logStorage.truncateFromIndex(5);

        IntStream.rangeClosed(1, 4).forEach(i -> {
            String value = "abc" + i;
            assertIndexLogEntry(logStorage.getEntryByIndex(i), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
            assertThat(logStorage.getTermByIndex(i)).isEqualTo(i);
        });
        assertThat(logStorage.getLastEntry().get().getIndex()).isEqualTo(4);
        assertThat(logStorage.getEntryByIndex(5)).isEmpty();
    }

    @Test
    void truncateManySegmentsWhenReaderIsOpened() {
        logStorage = new LogStorage(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.bytes, 100)
                .build());

        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        LogStorageReader logStorageReader = logStorage.openReader();
        logStorageReader.reset(9);

        logStorage.truncateFromIndex(5);

        assertThat(logStorageReader.getCurrentIndex()).isEqualTo(4);

        IntStream.rangeClosed(1, 4).forEach(i -> {
            String value = "abc" + i;
            assertIndexLogEntry(logStorage.getEntryByIndex(i), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
            assertThat(logStorage.getTermByIndex(i)).isEqualTo(i);
        });
        assertThat(logStorage.getLastEntry().get().getIndex()).isEqualTo(4);
        assertThat(logStorage.getEntryByIndex(5)).isEmpty();
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

    private CommandEntry commandEntry(int term, long timestamp, String value) {
        return new CommandEntry(term, timestamp, value.getBytes());
    }

    private void appendEntries(int numberOfEntries, Function<Integer, Long> timestampFunction, Function<Integer, String> valueFunction) {
        appendEntries(1, numberOfEntries, timestampFunction, valueFunction);
    }

    private void appendEntries(int rangeStart, int rangeEnd, Function<Integer, Long> timestampFunction, Function<Integer, String> valueFunction) {
        IntStream.rangeClosed(rangeStart, rangeEnd).forEach(i -> {
            logStorage.append(commandEntry(i, timestampFunction.apply(i), valueFunction.apply(i)));
        });
    }

    private IndexedLogEntry appendEntry(int i, Function<Integer, Long> timestampFunction, Function<Integer, String> valueFunction) {
        return logStorage.append(commandEntry(i, timestampFunction.apply(i), valueFunction.apply(i)));
    }
}