package io.github.pmackowski.rsocket.raft.storage.log.reader;

import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.log.LogStorage;
import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
        logStorageReader = logStorage.openChunkReader(1);
    }

    @AfterEach
    void tearDown() {
        logStorage.close();
        logStorageReader.close();
    }

    @Test
    void readSegment() {
        long timestamp = System.currentTimeMillis();
        appendEntries(1, 100, entry -> timestamp + entry, entry -> "abc" + entry);
        int i = 0;
        while (logStorageReader.hasNext()) {
            i++;
            String value = "abc" + i;
            assertIndexLogEntry(logStorageReader.next(), commandEntry(i, timestamp + i, value), i, Integer.BYTES + Long.BYTES + value.length());
        }
        assertThat(i).isEqualTo(100);
        assertThat(logStorageReader.hasNext()).isFalse();
    }

    @Test
    void resetSegment() {
        long timestamp = System.currentTimeMillis();
        appendEntries(1, 100, entry -> timestamp + entry, entry -> "abc" + entry);
        int i = 0;
        while (logStorageReader.hasNext() && i < 10) {
            i++;
            String value = "abc" + i;
            assertIndexLogEntry(logStorageReader.next(), commandEntry(i, timestamp + i, value), i, Integer.BYTES + Long.BYTES + value.length());
        }
        assertThat(i).isEqualTo(10);

        logStorageReader.reset(20);

        i = 19;
        while (logStorageReader.hasNext()) {
            i++;
            String value = "abc" + i;
            assertIndexLogEntry(logStorageReader.next(), commandEntry(i, timestamp + i, value), i, Integer.BYTES + Long.BYTES + value.length());
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