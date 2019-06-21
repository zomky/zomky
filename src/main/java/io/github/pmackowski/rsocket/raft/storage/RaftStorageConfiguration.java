package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;

import java.nio.file.Path;
import java.nio.file.Paths;

public class RaftStorageConfiguration {

    private static final String DEFAULT_DIRECTORY = System.getProperty("rsocket.raft.dir", System.getProperty("user.dir"));
    private static final int DEFAULT_SEGMENT_SIZE = SizeUnit.megabytes.getValue() * 16;
    private static final int DEFAULT_SEGMENT_INDEX_SIZE = DEFAULT_SEGMENT_SIZE / 4;
    private static final int DEFAULT_CHUNK_SIZE = SizeUnit.kilobytes.getValue() * 8;

    private Path directory;
    private int segmentSize;
    private int segmentIndexSize;
    private int chunkSize;

    public Path getDirectory() {
        return directory;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public int getSegmentIndexSize() {
        return segmentIndexSize;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    private RaftStorageConfiguration() {}

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Path directory = Paths.get(DEFAULT_DIRECTORY);
        private int segmentSize = DEFAULT_SEGMENT_SIZE;
        private int segmentIndexSize = DEFAULT_SEGMENT_INDEX_SIZE;
        private int chunkSize = DEFAULT_CHUNK_SIZE;

        private Builder() {
        }

        public Builder directory(String directory) {
            this.directory = Paths.get(directory);
            return this;
        }

        public Builder directory(Path directory) {
            this.directory = directory;
            return this;
        }

        public Builder segmentSize(SizeUnit sizeUnit, int size) {
            this.segmentSize = size * sizeUnit.getValue();
            return this;
        }

        public Builder segmentIndexSize(SizeUnit sizeUnit, int size) {
            this.segmentIndexSize = size * sizeUnit.getValue();
            return this;
        }

        public Builder chunkSize(SizeUnit sizeUnit, int size) {
            this.chunkSize = size * sizeUnit.getValue();
            return this;
        }

        public RaftStorageConfiguration build() {
            RaftStorageConfiguration raftStorageConfiguration = new RaftStorageConfiguration();
            raftStorageConfiguration.directory = directory;
            raftStorageConfiguration.segmentSize = segmentSize;
            raftStorageConfiguration.segmentIndexSize = segmentIndexSize;
            raftStorageConfiguration.chunkSize = chunkSize;
            return raftStorageConfiguration;
        }
    }

}
