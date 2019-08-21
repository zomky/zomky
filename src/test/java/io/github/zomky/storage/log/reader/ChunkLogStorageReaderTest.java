package io.github.zomky.storage.log.reader;

import io.github.zomky.storage.RaftStorageConfiguration;
import io.github.zomky.storage.log.LogStorage;
import io.github.zomky.storage.log.SizeUnit;
import io.github.zomky.storage.log.entry.CommandEntry;
import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class ChunkLogStorageReaderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkLogStorageReaderTest.class);

    private static final int SEGMENT_SIZE = 256;

    @TempDir
    Path directory;

    LogStorage logStorage;
    LogStorageReader logStorageReader;

    @BeforeEach
    void setUp() {
        LOGGER.info("directory {}", directory);
        logStorage = new LogStorage(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.bytes, SEGMENT_SIZE)
                .build()
        );
    }

    @AfterEach
    void tearDown() {
        logStorage.close();
        logStorageReader.close();
    }

    @Test
    void iterateLogThatHasManySegments() {
        logStorageReader = logStorage.openReader(1);

        long timestamp = System.currentTimeMillis();
        appendEntries(1, 100, entry -> timestamp + entry, entry -> "abc" + entry);
        int i = 0;
        while (logStorageReader.hasNext()) {
            i++;
            String value = "abc" + i;
            assertIndexLogEntry(logStorageReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        }
        assertThat(i).isEqualTo(100);
        assertThat(logStorageReader.hasNext()).isFalse();
    }

    @Test
    void iterateLogThatHasManySegmentsAndCurrentMaxIndexSupplierIsProvided() {
        int maxIndex = 35;
        logStorageReader = logStorage.openReader(1, () -> (long) maxIndex);

        long timestamp = System.currentTimeMillis();
        appendEntries(1, 100, entry -> timestamp + entry, entry -> "abc" + entry);
        int i = 0;
        while (logStorageReader.hasNext()) {
            i++;
            String value = "abc" + i;
            assertIndexLogEntry(logStorageReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        }
        assertThat(i).isEqualTo(maxIndex);
        assertThat(logStorageReader.hasNext()).isFalse();
    }

    @Test
    void resetLogThatHasManySegments() {
        logStorageReader = logStorage.openReader(1);

        long timestamp = System.currentTimeMillis();
        appendEntries(1, 100, entry -> timestamp + entry, entry -> "abc" + entry);
        int i = 0;
        while (logStorageReader.hasNext() && i < 10) {
            i++;
            String value = "abc" + i;
            assertIndexLogEntry(logStorageReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        }
        assertThat(i).isEqualTo(10);

        logStorageReader.reset(35);

        i = 34;
        while (logStorageReader.hasNext()) {
            i++;
            String value = "abc" + i;
            assertIndexLogEntry(logStorageReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        }
        assertThat(i).isEqualTo(100);


        assertThat(logStorageReader.hasNext()).isFalse();
    }

    @Test // TODO
    @Disabled
    void resetLogThatHasManySegmentsSameSegment() {
        logStorageReader = logStorage.openReader(1);

        long timestamp = System.currentTimeMillis();
        appendEntries(1, 100, entry -> timestamp + entry, entry -> "abc" + entry);
        int i = 0;
        while (logStorageReader.hasNext() && i < 10) {
            i++;
            String value = "abc" + i;
            assertIndexLogEntry(logStorageReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        }
        assertThat(i).isEqualTo(10);

        logStorageReader.reset(20);

        i = 19;
        while (logStorageReader.hasNext()) {
            i++;
            String value = "abc" + i;
            assertIndexLogEntry(logStorageReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        }
        assertThat(i).isEqualTo(100);


        assertThat(logStorageReader.hasNext()).isFalse();
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

    private void appendEntries(int rangeStart, int rangeEnd, Function<Integer, Long> timestampFunction, Function<Integer, String> valueFunction) {
        IntStream.rangeClosed(rangeStart, rangeEnd).forEach(i -> {
            logStorage.append(commandEntry(i, timestampFunction.apply(i), valueFunction.apply(i)));
        });
    }

}