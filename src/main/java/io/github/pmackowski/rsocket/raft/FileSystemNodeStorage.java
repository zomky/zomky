package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.FileSystemRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;

import java.nio.file.Path;
import java.nio.file.Paths;

public class FileSystemNodeStorage implements NodeStorage {

    private static final String DEFAULT_DIRECTORY = System.getProperty("rsocket.raft.dir", System.getProperty("user.dir"));

    private Path directory;

    public FileSystemNodeStorage(Path directory) {
        this.directory = directory;
    }

    public FileSystemNodeStorage(String directory) {
        this.directory = Paths.get(directory);
    }

    @Override
    public RaftStorage openRaftStorage(String groupName) {
        RaftStorageConfiguration storageConfiguration = RaftStorageConfiguration.builder()
                .directory(Paths.get(directory.toAbsolutePath().toString(), groupName))
                .build();
        return new FileSystemRaftStorage(storageConfiguration);
    }
}
