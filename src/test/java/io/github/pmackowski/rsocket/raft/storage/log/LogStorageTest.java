package io.github.pmackowski.rsocket.raft.storage.log;

import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.StorageException;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferUnderflowException;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        assertIndexLogEntry(actual, commandEntry(i, timestamp + i, value), i, Integer.BYTES + Long.BYTES + value.length());
        assertIndexLogEntry(logStorage.getEntryByIndex(1), commandEntry(i, timestamp + i, value), i, Integer.BYTES + Long.BYTES + value.length());
        assertThat(logStorage.getTermByIndex(1)).isEqualTo(1);
        assertThat(logStorage.getLastEntry()).isEqualTo(actual);
    }

    @Test
    void appendToExistingSegment() {
        logStorage = new LogStorage(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.kilobytes, 8)
                .build());

        long timestamp = System.currentTimeMillis();

        IndexedLogEntry actual = appendEntry(1, entry -> timestamp + entry, entry -> "abc1");
        assertIndexLogEntry(actual, commandEntry(1, timestamp + 1, "abc1"), 1, Integer.BYTES + Long.BYTES + "abc1".length());
        logStorage.close();

        logStorage = new LogStorage(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.kilobytes, 8)
                .build());

        actual = appendEntry(2, entry -> timestamp + entry, entry -> "abc2");
        assertIndexLogEntry(actual, commandEntry(2, timestamp + 2, "abc2"), 2, Integer.BYTES + Long.BYTES + "abc2".length());

        assertIndexLogEntry(logStorage.getEntryByIndex(2), commandEntry(2, timestamp + 2, "abc2"), 2, Integer.BYTES + Long.BYTES + "abc2".length());
        assertThat(logStorage.getTermByIndex(2)).isEqualTo(2);
        assertThat(logStorage.getLastEntry()).isEqualTo(actual);
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
            assertIndexLogEntry(logStorage.getEntryByIndex(i), commandEntry(i, timestamp + i, value), i, Integer.BYTES + Long.BYTES + value.length());
            assertThat(logStorage.getTermByIndex(i)).isEqualTo(i);
        });
        assertThat(logStorage.getLastEntry().getIndex()).isEqualTo(numberOfEntries);
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
            assertIndexLogEntry(logStorage.getEntryByIndex(i), commandEntry(i, timestamp + i, value), i, Integer.BYTES + Long.BYTES + value.length());
            assertThat(logStorage.getTermByIndex(i)).isEqualTo(i);
        });
        assertThat(logStorage.getLastEntry().getIndex()).isEqualTo(4);
        assertThatThrownBy(() -> logStorage.getEntryByIndex(5))
                .isInstanceOf(StorageException.class)
                .hasCause(new BufferUnderflowException());
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