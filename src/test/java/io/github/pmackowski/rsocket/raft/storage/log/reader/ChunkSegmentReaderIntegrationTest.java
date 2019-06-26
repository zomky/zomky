package io.github.pmackowski.rsocket.raft.storage.log.reader;

import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.log.Segment;
import io.github.pmackowski.rsocket.raft.storage.log.SegmentWriter;
import io.github.pmackowski.rsocket.raft.storage.log.Segments;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ChunkSegmentReaderIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkSegmentReaderTest.class);

    private static final int SEGMENT_SIZE = 32;

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
    void concurrentWriteAndRead() {
        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);

        long timestamp = System.currentTimeMillis();
        int numberOfEntries = 1000_000;

        int chunkSize = 4 * 1024;
        segmentReader = new ChunkSegmentReader(firstSegment, chunkSize);

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
                        assertIndexLogEntry(next, commandEntry(jj, timestamp + jj, value), jj, Integer.BYTES + Long.BYTES + value.length());
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
