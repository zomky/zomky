package io.github.pmackowski.rsocket.raft.storage.log;

import io.github.pmackowski.rsocket.raft.storage.StorageException;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static io.github.pmackowski.rsocket.raft.storage.RaftStorageUtils.getInt;
import static io.github.pmackowski.rsocket.raft.storage.RaftStorageUtils.openChannel;
import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;

public class Segment {

    private static final Logger LOGGER = LoggerFactory.getLogger(Segment.class);

    static final String SEGMENT_FILE = "segment_%d.log";
    static final String SEGMENT_INDEX_FILE = "index_%d.log";

    private final Path directory;
    private final SegmentId segmentId;
    private SegmentHeader segmentHeader;
    private SegmentEntryReader segmentEntryReader;

    public Segment(Path directory, SegmentHeader segmentHeader) {
        this.directory = directory;
        this.segmentId = SegmentId.of(segmentHeader.getId());
        this.segmentHeader = segmentHeader;
        this.segmentEntryReader = new SegmentEntryReader(this);
    }

    public SegmentId getSegmentId() {
        return segmentId;
    }

    public long getFirstIndex() {
        return segmentHeader.getFirstIndex();
    }

    public long getLastIndex() {
        int entriesCount = segmentEntryReader.entriesCount();
        return entriesCount > 0 ? segmentHeader.getFirstIndex() + entriesCount - 1 : 0;
    }

    public boolean isFirstSegment() {
        return getFirstIndex() == 1;
    }

    public int entriesCount() {
        return segmentEntryReader.entriesCount();
    }

    public Path getSegmentPath() {
        return toPath(SEGMENT_FILE);
    }

    public Path getSegmentIndexPath() {
        return toPath(SEGMENT_INDEX_FILE);
    }

    public SegmentHeader getSegmentHeader() {
        return segmentHeader;
    }

    public Optional<IndexedLogEntry> getEntryByIndex(long index) {
        if (index == 0) {
            return Optional.empty();
        }
        return segmentEntryReader.getEntryByIndex(index);
    }

    public Optional<IndexedLogEntry> getLastEntry() {
        final long lastIndex = getLastIndex();
        return getEntryByIndex(lastIndex);
    }

    private Path toPath(String file) {
        return Paths.get(directory.toAbsolutePath().toString(), String.format(file, segmentId.getSegmentId()));
    }

    public void release() {
        segmentEntryReader.release();
    }

    private class SegmentEntryReader {

        private FileChannel segmentChannel;
        private FileChannel segmentIndexChannel;
        private long firstIndex;

        SegmentEntryReader(Segment segment) {
            this.segmentChannel = openChannel(segment.getSegmentPath());
            this.segmentIndexChannel = openChannel(segment.getSegmentIndexPath());
            this.firstIndex = segment.getFirstIndex();
        }

        Optional<IndexedLogEntry> getEntryByIndex(long index) {
            try {
                int position = getInt(segmentIndexChannel, (index - firstIndex) * Integer.BYTES);
                if (position == 0) {
                    return Optional.empty();
                }
                int entrySize = getInt(segmentChannel, position);
                ByteBuffer entryBuffer = ByteBuffer.allocate(entrySize);
                segmentChannel.read(entryBuffer, position + Integer.BYTES);
                entryBuffer.flip();
                LogEntry logEntry = deserialize(entryBuffer, entrySize);
                return Optional.of(new IndexedLogEntry(logEntry, index, entrySize));
            } catch (BufferUnderflowException|IllegalArgumentException e) {
                LOGGER.warn(String.format("getEntryByIndex %s failed!", index), e);
                return Optional.empty();
            } catch (Exception e) {
                throw new StorageException(e);
            }
        }

        int entriesCount() {
            if (getInt(segmentIndexChannel, 0) == 0) {
                return 0;
            }
            int maxEntries = maxEntries();
            int left = 0, right = maxEntries - 1;

            while (left <= right) {
                int mid = (left + right) / 2;
                int midValue = getInt(segmentIndexChannel, mid * Integer.BYTES);
                if (midValue == 0 && (mid == 0 || getInt(segmentIndexChannel, (mid - 1) * Integer.BYTES) > 0)) {
                    return Math.max(mid, 0);
                } else if (midValue == 0) {
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }
            return maxEntries;
        }

        private int maxEntries() {
            try {
                return (int) segmentIndexChannel.size() / Integer.BYTES;
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }

        public void release() {
            try {
                segmentChannel.close();
                segmentIndexChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return "segment[id = " + segmentId + ", header = " + segmentHeader + '}';
    }
}
