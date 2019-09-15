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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferOverflowException;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SegmentReaderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentReaderTest.class);

    private static final int SEGMENT_SIZE = 8 * 1024;

    @TempDir
    Path directory;

    Segments segments;
    Segment firstSegment;
    SegmentWriter segmentWriter;
    SegmentReader segmentReader;

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

    private static Stream<Function<Segment,SegmentReader>> noLogEntriesReaders() {
        return Stream.of(
                MappedSegmentReader::new,
                segment -> new ChunkSegmentReader(segment, SEGMENT_SIZE / 4)
        );
    }

    @ParameterizedTest
    @MethodSource("noLogEntriesReaders")
    void noLogEntries(Function<Segment,SegmentReader> func) {
        segmentReader = func.apply(firstSegment);

        assertThat(segmentReader.hasNext()).isFalse();
        assertThat(segmentReader.hasNext()).isFalse();
        assertThatThrownBy(() -> segmentReader.next())
                .isInstanceOf(NoSuchElementException.class);
    }

    private static Stream<Function<Segment,SegmentReader>> logEntriesPreInitializedReaders() {
        return Stream.of(
                MappedSegmentReader::new,
                segment -> new ChunkSegmentReader(segment, SEGMENT_SIZE / 4)
        );
    }

    @ParameterizedTest
    @MethodSource("logEntriesPreInitializedReaders")
    void logEntriesPreInitialized(Function<Segment,SegmentReader> func) {
        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        segmentReader = func.apply(firstSegment);

        assertThat(segmentReader.getCurrentIndex()).isEqualTo(1);
        IntStream.rangeClosed(1, numberOfEntries).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value = "abc" + i;
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        });

        assertThat(segmentReader.hasNext()).isFalse();
        assertThat(segmentReader.getCurrentIndex()).isEqualTo(numberOfEntries);
    }

    private static Stream<Function<Segment,SegmentReader>> logEntriesPreInitializedAndCurrentMaxIndexSupplierIsProvidedReaders() {
        return Stream.of(
                segment -> new MappedSegmentReader(segment, () -> 5L),
                segment -> new ChunkSegmentReader(segment, SEGMENT_SIZE / 4, () -> 5L)
        );
    }

    @ParameterizedTest
    @MethodSource("logEntriesPreInitializedAndCurrentMaxIndexSupplierIsProvidedReaders")
    void logEntriesPreInitializedAndCurrentMaxIndexSupplierIsProvided(Function<Segment,SegmentReader> func) {
        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        int maxIndex = 5;
        segmentReader = func.apply(firstSegment);
        assertThat(segmentReader.getCurrentIndex()).isEqualTo(1);

        IntStream.rangeClosed(1, maxIndex).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value = "abc" + i;
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        });
        assertThat(segmentReader.getCurrentIndex()).isEqualTo(maxIndex);

        assertThat(segmentReader.hasNext()).isFalse();
    }

    private static Stream<Function<Segment,SegmentReader>> logEntriesPreInitializedAndIndexGreaterThanFirstIndexProvidedReaders() {
        return Stream.of(
                segment -> new MappedSegmentReader(segment, 5L),
                segment -> new ChunkSegmentReader(segment, 5L, SEGMENT_SIZE / 4)
        );
    }

    @ParameterizedTest
    @MethodSource("logEntriesPreInitializedAndIndexGreaterThanFirstIndexProvidedReaders")
    void logEntriesPreInitializedAndIndexGreaterThanFirstIndex(Function<Segment,SegmentReader> func) {
        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        int index = 5;
        segmentReader = func.apply(firstSegment);

        assertThat(segmentReader.getCurrentIndex()).isEqualTo(5);
        IntStream.rangeClosed(index, numberOfEntries).forEach(i -> {
            assertThat(segmentReader.hasNext()).isTrue();
            String value = "abc" + i;
            assertIndexLogEntry(segmentReader.next(), commandEntry(i, timestamp + i, value), i, LogEntry.SIZE + 1 + value.length());
        });
        assertThat(segmentReader.getCurrentIndex()).isEqualTo(numberOfEntries);
        assertThat(segmentReader.hasNext()).isFalse();
    }

    private static Stream<Function<Segment,SegmentReader>> logEntriesPreInitializedAndIndexGreaterThanFirstIndexAndCurrentMaxIndexSupplierIsProvidedReaders() {
        return Stream.of(
                segment -> new MappedSegmentReader(segment, 5L, () -> 7L),
                segment -> new ChunkSegmentReader(segment, 5L, SEGMENT_SIZE / 4, () -> 7L)
        );
    }

    @ParameterizedTest
    @MethodSource("logEntriesPreInitializedAndIndexGreaterThanFirstIndexAndCurrentMaxIndexSupplierIsProvidedReaders")
    void logEntriesPreInitializedAndIndexGreaterThanFirstIndexAndCurrentMaxIndexSupplierIsProvided(Function<Segment,SegmentReader> func) {
        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;
        appendEntries(numberOfEntries, entry -> timestamp + entry, entry -> "abc" + entry);

        int index = 5;
        int maxIndex = 7;
        segmentReader = func.apply(firstSegment);

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

    private static Stream<Function<Segment,SegmentReader>> logEntriesFullyPreInitializedReaders() {
        return Stream.of(
                MappedSegmentReader::new,
                segment -> new ChunkSegmentReader(segment, 104)
        );
    }

    @ParameterizedTest
    @MethodSource("logEntriesFullyPreInitializedReaders")
    void logEntriesFullyPreInitialized(Function<Segment,SegmentReader> func) {
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

        segmentReader = func.apply(firstSegment);

        int j = 1;
        while (segmentReader.hasNext()) {
            String value = "abc" + j;
            assertIndexLogEntry(segmentReader.next(), commandEntry(j, timestamp + j, value), j, LogEntry.SIZE + 1 + value.length());
            j++;
        }
        assertThat(segmentReader.hasNext()).isFalse();
    }

    private static Stream<BiFunction<Segment,Long, SegmentReader>> logEntriesFullyPreInitializedAndReadLastCoupleOfEntriesReaders() {
        return Stream.of(
                MappedSegmentReader::new,
                (segment,index) -> new ChunkSegmentReader(segment, index,SEGMENT_SIZE / 4)
        );
    }

    @ParameterizedTest
    @MethodSource("logEntriesFullyPreInitializedAndReadLastCoupleOfEntriesReaders")
    void logEntriesFullyPreInitializedAndReadLastCoupleOfEntries(BiFunction<Segment,Long, SegmentReader> func) {
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

        int lastEntries = 5;
        segmentReader = func.apply(firstSegment, lastIndex - lastEntries);

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

    private static Stream<Function<Segment,SegmentReader>> logEntriesAddedAfterReaderInitializationReaders() {
        return Stream.of(
                MappedSegmentReader::new,
                segment -> new ChunkSegmentReader(segment, 2 * 1024)
        );
    }


    @ParameterizedTest
    @MethodSource("logEntriesAddedAfterReaderInitializationReaders")
    void logEntriesAddedAfterReaderInitialization(Function<Segment,SegmentReader> func) {
        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 10;

        segmentReader = func.apply(firstSegment);

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