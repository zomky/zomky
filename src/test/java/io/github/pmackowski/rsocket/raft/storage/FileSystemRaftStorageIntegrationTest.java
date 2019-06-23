package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemRaftStorageIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemRaftStorageIntegrationTest.class);

    @TempDir
    Path directory;

    private FileSystemRaftStorage raftStorage;

    @BeforeEach
    void setUp() {
        LOGGER.info("directory {}", directory);
    }

    @AfterEach
    void tearDown() {
        raftStorage.close();
    }

    @Test
    void storageAfterInitialization() {
        RaftStorageConfiguration configuration = RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.kilobytes, 32)
                .build();

        raftStorage = new FileSystemRaftStorage(configuration);

        assertThat(raftStorage.getVotedFor()).isEqualTo(0);
        assertThat(raftStorage.getTerm()).isEqualTo(0);
        assertThat(raftStorage.getLast().getIndex()).isEqualTo(0);
        assertThat(raftStorage.getTermByIndex(0)).isEqualTo(0);
    }

    @Test
    void updateTermAndVotedFor() {
        RaftStorageConfiguration configuration = RaftStorageConfiguration.builder()
                .directory(directory)
                .segmentSize(SizeUnit.kilobytes, 32)
                .build();

        raftStorage = new FileSystemRaftStorage(configuration);

        raftStorage.update(1, 7001);

        assertThat(raftStorage.getVotedFor()).isEqualTo(7001);
        assertThat(raftStorage.getTerm()).isEqualTo(1);
        assertThat(raftStorage.getLast().getIndex()).isEqualTo(0);
        assertThat(raftStorage.getTermByIndex(0)).isEqualTo(0);
    }

}