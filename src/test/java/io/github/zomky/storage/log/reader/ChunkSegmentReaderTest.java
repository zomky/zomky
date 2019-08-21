package io.github.zomky.storage.log.reader;

import io.github.zomky.storage.RaftStorageConfiguration;
import io.github.zomky.storage.log.*;
import io.github.zomky.storage.log.entry.CommandEntry;
import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferOverflowException;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChunkSegmentReaderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkSegmentReaderTest.class);

    private static final int SEGMENT_SIZE = 8 * 1024;

    @TempDir
    Path directory;

    Segments segments;
    Segment firstSegment;
    SegmentWriter segmentWriter;
    ChunkSegmentReader segmentReader;

    @BeforeEach
    void setUp() {
        LOGGER.info("directory {}", directory);
        segments = new Segments(RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.bytes, SEGMENT_SIZE)
                .build()
        );
        firstSegment = segments.getLastSegment();
        segmentWriter = new SegmentWriter(firstSegment);

    }

    @AfterEach
    void tearDown() {
        segments.release();
        if (segmentReader != null)
            segmentReader.close();
        segmentWriter.release();
    }

    @Test
    void noLogEntries() {
        segmentReader = new ChunkSegmentReader(firstSegment, SEGMENT_SIZE / 4);

        assertThat(segmentReader.hasNext()).isFalse();
        assertThat(segmentReader.hasNext()).isFalse();
        assertThatThrownBy(() -> segmentReader.next())
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void logEntriesPreInitialized() {
        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        int chunkSize = SEGMENT_SIZE / 4;
        segmentReader = new ChunkSegmentReader(firstSegment, chunkSize);

        assertThat(segmentReader.getCurrentIndex()).isEqualTo(1);
        IntStream.rangeClosed(1, numberOfEntries).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value = "abc" + i;
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        });

        assertThat(segmentReader.hasNext()).isFalse();
        assertThat(segmentReader.getCurrentIndex()).isEqualTo(numberOfEntries);
    }

    @Test
    void logEntriesPreInitializedAndCurrentMaxIndexSupplierIsProvided() {
        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        int chunkSize = SEGMENT_SIZE / 4;
        int maxIndex = 5;
        segmentReader = new ChunkSegmentReader(firstSegment, chunkSize, () -> (long) maxIndex);
        assertThat(segmentReader.getCurrentIndex()).isEqualTo(1);

        IntStream.rangeClosed(1, maxIndex).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value = "abc" + i;
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        });
        assertThat(segmentReader.getCurrentIndex()).isEqualTo(maxIndex);

        assertThat(segmentReader.hasNext()).isFalse();
    }

    @Test
    void logEntriesPreInitializedAndIndexGreaterThanFirstIndex() {
        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        int chunkSize = SEGMENT_SIZE / 4;
        int index = 5;
        segmentReader = new ChunkSegmentReader(firstSegment, index, chunkSize);

        assertThat(segmentReader.getCurrentIndex()).isEqualTo(5);
        IntStream.rangeClosed(index, numberOfEntries).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value = "abc" + i;
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        });
        assertThat(segmentReader.getCurrentIndex()).isEqualTo(numberOfEntries);
        assertThat(segmentReader.hasNext()).isFalse();
    }


    @Test
    void logEntriesPreInitializedAndIndexGreaterThanFirstIndexAndCurrentMaxIndexSupplierIsProvided() {
        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        int chunkSize = SEGMENT_SIZE / 4;
        int index = 5;
        int maxIndex = 7;
        segmentReader = new ChunkSegmentReader(firstSegment, index, chunkSize, () -> (long) maxIndex);

        IntStream.rangeClosed(index, maxIndex).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value = "abc" + i;
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        });
        assertThat(segmentReader.getCurrentIndex()).isEqualTo(maxIndex);
        assertThat(segmentReader.hasNext()).isFalse();
    }

    @Test
    void logEntriesPreInitializedAndChunkSizeExceeded() {
        long timestamp = System.currentTimeMillis();
        // all entries are of the same size equals to 25
        // entry position = 4 bytes
        // entry          = 22 bytes
        //  term      = 4 bytes = Integer.BYTES
        //  timestamp = 8 bytes = Long.BYTES
        //  value     = 9 bytes (abc100000 - abc100010) + 1 byte (entry type)
        int entrySize = 22;
        int entryPositionSize = Integer.BYTES;
        int rangeStart = 100_000;
        int rangeEnd = rangeStart + 10;
        appendEntries(rangeStart, rangeEnd, entry -> timestamp + entry, entry -> "abc" + entry);

        int chunkSize = (entrySize + entryPositionSize) * 4; // space for exactly 4 entries
        segmentReader = new ChunkSegmentReader(firstSegment, chunkSize);

        IntStream.rangeClosed(rangeStart, rangeEnd).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value = "abc" + i;
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i - rangeStart + 1, LogEntry.SIZE + 1 + value.length());
        });

        assertThat(segmentReader.hasNext()).isFalse();
    }

    @Test
    void logEntriesPreInitializedAndChunkSizeExceededWithSliceGreaterOrEqualThan4Bytes() {
        long timestamp = System.currentTimeMillis();
        // all entries are of the same size equals to 25
        // entry position = 4 bytes
        // entry          = 21 bytes
        //  term      = 4 bytes = Integer.BYTES
        //  timestamp = 8 bytes = Long.BYTES
        //  value     = 9 bytes (abc100000 - abc100010) + 1 byte (entry type)
        int entrySize = 22;
        int entryPositionSize = Integer.BYTES;
        int rangeStart = 100_000;
        int rangeEnd = rangeStart + 10;
        appendEntries(rangeStart, rangeEnd, entry -> timestamp + entry, entry -> "abc" + entry);

        // space for 4 entries, 10 bytes in first iteration should be sliced
        int chunkSize = (entrySize + entryPositionSize) * 4 + 10;
        segmentReader = new ChunkSegmentReader(firstSegment, chunkSize);

        IntStream.rangeClosed(rangeStart, rangeEnd).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value = "abc" + i;
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i - rangeStart + 1, LogEntry.SIZE + 1 + value.length());
        });

        assertThat(segmentReader.hasNext()).isFalse();
    }

    @Test
    void logEntriesPreInitializedAndChunkSizeExceededWithSliceSmallerThan4Bytes() {
        long timestamp = System.currentTimeMillis();
        // all entries are of the same size equals to 25
        // entry position = 4 bytes
        // entry          = 22 bytes
        //  term      = 4 bytes = Integer.BYTES
        //  timestamp = 8 bytes = Long.BYTES
        //  value     = 9 bytes (abc100000 - abc100010) + 1 byte (entry type)
        int entrySize = 21;
        int entryPositionSize = Integer.BYTES;
        int rangeStart = 100_000;
        int rangeEnd = rangeStart + 10;
        appendEntries(rangeStart, rangeEnd, entry -> timestamp + entry, entry -> "abc" + entry);

        // space for 4 entries, 2 bytes in first iteration should be sliced
        int chunkSize = (entrySize + entryPositionSize) * 4 + 2;
        segmentReader = new ChunkSegmentReader(firstSegment, chunkSize);

        IntStream.rangeClosed(rangeStart, rangeEnd).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value = "abc" + i;
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i - rangeStart + 1, LogEntry.SIZE + 1 + value.length());
        });

        assertThat(segmentReader.hasNext()).isFalse();
    }

    @Test
    void logEntriesFullyPreInitialized() {
        long timestamp = System.currentTimeMillis();
        int entryPositionSize = Integer.BYTES;

        int i = 1;
        int totalSize = 0;
        try {
            for (;;) {
                IndexedLogEntry indexedLogEntry = appendEntry(i++, entry -> timestamp + entry, entry -> "abc" + entry);
                totalSize = totalSize + indexedLogEntry.getSize() + entryPositionSize;
            }
        } catch (BufferOverflowException e) {
            LOGGER.info("Segment size without header {}. Written data size {}", SEGMENT_SIZE - SegmentHeader.SIZE, totalSize);
        }

        int chunkSize = 104;
        segmentReader = new ChunkSegmentReader(firstSegment, chunkSize);

        int j = 1;
        while (segmentReader.hasNext()) {
            String value = "abc" + j;
            assertIndexLogEntry(segmentReader.next(), commandEntry(j, timestamp + j, value), j, LogEntry.SIZE + 1 + value.length());
            j++;
        }
        assertThat(segmentReader.hasNext()).isFalse();
    }

    @Test
    void logEntriesFullyPreInitializedAndReadLastCoupleOfEntries() {
        long timestamp = System.currentTimeMillis();
        int entryPositionSize = Integer.BYTES;

        int i = 1;
        int totalSize = 0;
        long lastIndex = 0;
        try {
            for (;;) {
                IndexedLogEntry indexedLogEntry = appendEntry(i++, entry -> timestamp + entry, entry -> "abc" + entry);
                totalSize = totalSize + indexedLogEntry.getSize() + entryPositionSize;
                lastIndex = indexedLogEntry.getIndex();
            }
        } catch (BufferOverflowException e) {
            LOGGER.info("Segment size without header {}. Written data size {}. Last index {}", SEGMENT_SIZE - SegmentHeader.SIZE, totalSize, lastIndex);
        }

        int chunkSize = SEGMENT_SIZE / 4;
        int lastEntries = 5;
        segmentReader = new ChunkSegmentReader(firstSegment, lastIndex - lastEntries, chunkSize);

        long j = lastIndex - lastEntries;
        while (segmentReader.hasNext()) {
            String value = "abc" + j;
            assertIndexLogEntry(segmentReader.next(), commandEntry((int) j, timestamp + j, value), j, LogEntry.SIZE + 1 + value.length());
            j++;
        }
        assertThat(segmentReader.hasNext()).isFalse();
    }

    @Test
    void logEntriesPreInitializedAndSomeEntryGreaterThanChunkSize() {
        long timestamp = System.currentTimeMillis();
        // all entries are of the same size equals to 25 except 4rd and 7th entries
        // entry position = 4 bytes
        // entry          = 21 bytes
        //  term      = 4 bytes = Integer.BYTES
        //  timestamp = 8 bytes = Long.BYTES
        //  value     = 9 bytes (abc100000 - abc100010) + 1 byte (entry type)
        int entrySize = 22;
        int entryPositionSize = Integer.BYTES;
        int rangeStart = 100_000;
        int rangeEnd = rangeStart + 10;
        appendEntries(rangeStart, rangeEnd, entry -> timestamp + entry, entry -> {
            if (entry == rangeStart + 3) {
                return String.format("%109c", 'a');
            }
            if (entry == rangeStart + 6) {
                return String.format("%120c", 'a');
            }
            return "abc" + entry;
        });

        int chunkSize = (entrySize + entryPositionSize) * 4;
        segmentReader = new ChunkSegmentReader(firstSegment, chunkSize); // space for exactly 4 entries

        IntStream.rangeClosed(rangeStart, rangeEnd).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value;
            if (i == rangeStart + 3) {
                value = String.format("%109c", 'a');
            } else if (i == rangeStart + 6) {
                value = String.format("%120c", 'a');
            } else {
                value = "abc" + i;
            }
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i - rangeStart + 1, LogEntry.SIZE + 1 + value.length());
        });

        assertThat(segmentReader.hasNext()).isFalse();
    }

    @Test
    void logEntriesAddedAfterReaderInitialization() {
        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;

        int chunkSize = 2 * 1024;
        segmentReader = new ChunkSegmentReader(firstSegment, chunkSize);

        assertThat(segmentReader.hasNext()).isFalse();

        appendEntries(1, numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        IntStream.rangeClosed(1, numberOfEntries).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value = "abc" + i;
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        });

        assertThat(segmentReader.hasNext()).isFalse();
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
            segmentWriter.appendEntry(commandEntry(i, timestampFunction.apply(i), valueFunction.apply(i)));
        });
    }

    private IndexedLogEntry appendEntry(int i, Function<Integer, Long> timestampFunction, Function<Integer, String> valueFunction) {
        return segmentWriter.appendEntry(commandEntry(i, timestampFunction.apply(i), valueFunction.apply(i)));
    }

}