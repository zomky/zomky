package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class RaftStorageTest {

    private static Stream<Function<Path,RaftStorage>> storages() {
        return Stream.of(
                    directory -> new InMemoryRaftStorage(),
                    directory -> new FileSystemRaftStorage(RaftStorageConfiguration.builder()
                                        .directory(directory)
                                        .segmentSize(SizeUnit.kilobytes, 16)
                                        .build()),
                    directory -> new FileSystemRaftStorage(RaftStorageConfiguration.builder()
                                        .directory(directory)
                                        .segmentSize(SizeUnit.bytes, 50)
                                        .build()
                )
        );
    }

    @ParameterizedTest
    @MethodSource("storages")
    void storageAfterInitialization(Function<Path,RaftStorage> func, @TempDir Path directory) {
        RaftStorage raftStorage = func.apply(directory);
        try {
            assertThat(raftStorage.getVotedFor()).isEqualTo(0);
            assertThat(raftStorage.getTerm()).isEqualTo(0);
            assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(0);
            assertThat(raftStorage.getTermByIndex(0)).isEqualTo(0);
        } finally {
            raftStorage.close();
        }
    }

    @ParameterizedTest
    @MethodSource("storages")
    void append(Function<Path, RaftStorage> func, @TempDir Path directory) {
        RaftStorage raftStorage = func.apply(directory);

        final CommandEntry c1 = commandEntry(1, "val1");
        final CommandEntry c2 = commandEntry(1, "val2");
        final CommandEntry c3 = commandEntry(1, "val3");

        raftStorage.append(c1);
        raftStorage.append(c2);
        raftStorage.append(c3);

        assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(3);
    }

    private CommandEntry commandEntry(int term, String value) {
        return new CommandEntry(term, System.currentTimeMillis(), value.getBytes());
    }
}
