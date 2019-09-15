package io.github.zomky.storage.log.reader;

import io.github.zomky.storage.RaftStorageConfiguration;
import io.github.zomky.storage.log.Segment;
import io.github.zomky.storage.log.SegmentWriter;
import io.github.zomky.storage.log.Segments;
import io.github.zomky.storage.log.SizeUnit;
import io.github.zomky.storage.log.entry.CommandEntry;
import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class SegmentReaderIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentReaderIntegrationTest.class);

    private static final int SEGMENT_SIZE = 32;

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
                .segmentSize(SizeUnit.megabytes, SEGMENT_SIZE)
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
    void concurrentWriteAndReadForMappedReader() {
        MappedSegmentReader segmentReader = new MappedSegmentReader(firstSegment);
        concurrentWriteAndRead(segmentReader);
    }

    @Test
    void concurrentWriteAndReadForChunkedReader() {
        int chunkSize = 4 * 1024;
        ChunkSegmentReader segmentReader = new ChunkSegmentReader(firstSegment, chunkSize);
        concurrentWriteAndRead(segmentReader);
    }

    private void concurrentWriteAndRead(SegmentReader segmentReader) {
        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);

        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 1000_000;

        // READER
        final AtomicInteger j = new AtomicInteger(0);
        scheduledExecutorService.execute(() -> {
            for (;;) {
                if (segmentReader.hasNext()) {
                    int jj = j.get() + 1;
                    String value = "abc" + jj;
                    final IndexedLogEntry next = segmentReader.next();
                    if (jj % 100_000 == 0) {
                        LOGGER.info("read {}", next);
                        assertIndexLogEntry(next, commandEntry(jj, timestamp + jj, value), jj, LogEntry.SIZE + 1 + value.length());
                    }
                    j.incrementAndGet();
                }
            }
        });

        // WRITER
        scheduledExecutorService.execute(() -> {
            int idx = 0;
            while (idx < numberOfEntries) {
                final IndexedLogEntry indexedLogEntry = appendEntry(idx + 1, entry -> timestamp + entry, entry -> "abc" + entry);
                if (indexedLogEntry.getIndex() % 100_000 == 0) {
                    LOGGER.info("write {}", indexedLogEntry);
                }
                idx = idx + 1;
            }
        });

        await().atMost(2, TimeUnit.SECONDS).until(() -> j.get() == numberOfEntries);

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

    private IndexedLogEntry appendEntry(int i, Function<Integer, Long> timestampFunction, Function<Integer, String> valueFunction) {
        return segmentWriter.appendEntry(commandEntry(i, timestampFunction.apply(i), valueFunction.apply(i)));
    }

}
