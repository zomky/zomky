package io.github.zomky.storage.log;

import io.github.zomky.storage.RaftStorageConfiguration;
import io.github.zomky.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static io.github.zomky.storage.RaftStorageUtils.openChannel;

public class SegmentsReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentsReader.class);

    public NavigableMap<Long, Segment> read(RaftStorageConfiguration configuration) {
        TreeMap<Integer, Segment> segments = new TreeMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(configuration.getDirectory())) {
            for (Path path : stream) {
                if (path.getFileName().toString().startsWith("segment")) {
                    ByteBuffer buffer = ByteBuffer.allocate(SegmentHeader.SIZE);

                    try (FileChannel channel = openChannel(path)) {
                        channel.read(buffer);
                        buffer.flip();
                    } catch (IOException e) {
                        throw new StorageException(e);
                    }

                    SegmentHeader segmentHeader = new SegmentHeader(buffer);
                    LOGGER.info("Segment {} has been loaded from storage", segmentHeader);
                    Segment segment = new Segment(configuration.getDirectory(), segmentHeader);
                    segments.put(segmentHeader.getId(), segment);
                }
            }
        } catch (IOException e) {
            throw new StorageException(e);
        }
        Segment previousSegment = null;
        for (Map.Entry<Integer, Segment> integerSegmentEntry : segments.entrySet()) {
            Segment segment = integerSegmentEntry.getValue();
            if (previousSegment != null && previousSegment.getLastIndex() != segment.getFirstIndex() - 1) {
                LOGGER.warn("Journal is inconsistent. {} is not aligned with prior segment {}", segment.getSegmentPath(), previousSegment.getSegmentPath());
            }
            previousSegment = segment;
        }

        NavigableMap<Long, Segment> segments2 = new ConcurrentSkipListMap<>();

        segments.forEach((id, segment) -> segments2.put(segment.getFirstIndex(), segment));
        return segments2;
    }

}
