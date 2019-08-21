package io.github.zomky;

import io.github.zomky.storage.FileSystemRaftStorage;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.RaftStorageConfiguration;

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
