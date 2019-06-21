package io.github.pmackowski.rsocket.raft.storage.log;

import java.util.Objects;

public class SegmentId {

    private int segmentId;

    private SegmentId(int segmentId) {
        this.segmentId = segmentId;
    }

    public static SegmentId of(int segmentId) {
        return new SegmentId(segmentId);
    }

    public int getSegmentId() {
        return segmentId;
    }

    @Override
    public String toString() {
        return "SegmentId{" +
                "segmentId=" + segmentId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SegmentId segmentId1 = (SegmentId) o;
        return segmentId == segmentId1.segmentId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentId);
    }
}
