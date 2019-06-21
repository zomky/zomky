package io.github.pmackowski.rsocket.raft.storage.log;

import java.nio.ByteBuffer;

public class SegmentHeader {

    public static final int SIZE = 24;

    public static final long SEGMENT_ID_POSITION = 0;
    public static final long SEGMENT_FIRST_INDEX_POSITION = SEGMENT_ID_POSITION + Integer.BYTES;
    public static final long SEGMENT_SEGMENT_SIZE_POSITION = SEGMENT_FIRST_INDEX_POSITION + Long.BYTES;

    private int id;
    private long firstIndex;
    private int segmentSize;

    public SegmentHeader(ByteBuffer buffer) {
        this.id = buffer.getInt();
        this.firstIndex = buffer.getLong();
        this.segmentSize = buffer.getInt();
    }

    ByteBuffer asBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(SegmentHeader.SIZE);
        buffer.putInt(id);
        buffer.putLong(firstIndex);
        buffer.putInt(segmentSize);
        buffer.flip();
        return buffer;
    }

    public int getId() {
        return id;
    }

    public long getFirstIndex() {
        return firstIndex;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public int getSegmentIndexSize() {
        return (segmentSize - SIZE) / 3;
    }

    private SegmentHeader() {}

    public static SegmentHeader.Builder builder() {
        return new SegmentHeader.Builder();
    }

    public static class Builder {
        private int id;
        private long firstIndex;
        private int segmentSize;

        private Builder() {
        }

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder firstIndex(long firstIndex) {
            this.firstIndex = firstIndex;
            return this;
        }

        public Builder segmentSize(int segmentSize) {
            this.segmentSize = segmentSize;
            return this;
        }

        public SegmentHeader build() {
            SegmentHeader segmentHeader = new SegmentHeader();
            segmentHeader.id = id;
            segmentHeader.firstIndex = firstIndex;
            segmentHeader.segmentSize = segmentSize;
            return segmentHeader;
        }
    }

    @Override
    public String toString() {
        return "SegmentHeader{" +
                "id=" + id +
                ", firstIndex=" + firstIndex +
                ", segmentSize=" + segmentSize +
                '}';
    }

}
