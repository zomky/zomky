package io.github.pmackowski.rsocket.raft.storage.log;

import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

public class Segments {

    private static final Logger LOGGER = LoggerFactory.getLogger(Segments.class);

    private final RaftStorageConfiguration configuration;
    private final NavigableMap<Long, Segment> segments;
    private final SegmentsWriter segmentsWriter;

    public Segments(RaftStorageConfiguration configuration) {
        this(configuration, new SegmentsReader(), new SegmentsWriter());
    }

    public Segments(RaftStorageConfiguration configuration, SegmentsReader segmentsReader, SegmentsWriter segmentsWriter) {
        this.configuration = configuration;
        this.segmentsWriter = segmentsWriter;
        this.segments = segmentsReader.read(configuration);
        if (segments.isEmpty()) {
            createFirstSegment();
        }
    }

    public Segment createFirstSegment() {
        SegmentHeader segmentHeader = SegmentHeader.builder()
                .id(1)
                .firstIndex(1)
                .segmentSize(configuration.getSegmentSize())
                .build();

        return createSegment(segmentHeader);
    }

    public Segment createNextSegment(long firstIndex) {
        Segment lastSegment = getLastSegment();
        SegmentHeader segmentHeader = SegmentHeader.builder()
                .id(lastSegment.getSegmentHeader().getId() + 1)
                .firstIndex(firstIndex)
                .segmentSize(configuration.getSegmentSize())
                .build();

        return createSegment(segmentHeader);
    }

    public Segment createNextSegment() {
        Segment lastSegment = getLastSegment();
        SegmentHeader segmentHeader = SegmentHeader.builder()
                .id(lastSegment.getSegmentHeader().getId() + 1)
                .firstIndex(lastSegment.getLastIndex() + 1)
                .segmentSize(configuration.getSegmentSize())
                .build();

        return createSegment(segmentHeader);
    }

    public Segment deleteSegments(long index) {
        getTailSegments(index).forEach(segment -> {
            segment.release();
            segmentsWriter.delete(segment);
            segments.remove(segment.getFirstIndex());
        });
        return getLastSegment();
    }

    public Segment getLastSegment() {
        return segments.lastEntry().getValue();
    }

    public Segment getSegment(long index) {
        Preconditions.checkState(index >= 1);
        return segments.floorEntry(index).getValue();
    }

    private Segment createSegment(SegmentHeader segmentHeader) {
        Segment segment = new Segment(configuration.getDirectory(), segmentHeader);
        segmentsWriter.createOrUpdate(segment);
        segments.put(segment.getFirstIndex(), segment);
        return segment;
    }

    public Segment getNextSegment(long index) {
        Map.Entry<Long, Segment> nextSegment = segments.higherEntry(index);
        return nextSegment != null ? nextSegment.getValue() : null;
    }

    Collection<Segment> getTailSegments(long index) {
        return segments.tailMap(index).values();
    }

    public void release() {
        segments.values().forEach(Segment::release);
    }

}
