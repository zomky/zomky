package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.ConfigurationEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.BoundedLogStorageReader;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class RaftStorageTest {

    private static Stream<Function<Path, RaftStorage>> storages() {
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
    void storageAfterInitialization(Function<Path, RaftStorage> func, @TempDir Path directory) {
        RaftStorage raftStorage = func.apply(directory);
        test(raftStorage, () -> {
            assertThat(raftStorage.getVotedFor()).isEqualTo(0);
            assertThat(raftStorage.getTerm()).isEqualTo(0);
            assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(0);
            assertThat(raftStorage.getTermByIndex(0)).isEqualTo(0);
        });
    }

    @ParameterizedTest
    @MethodSource("storages")
    void appendCommandEntry(Function<Path, RaftStorage> func, @TempDir Path directory) {
        RaftStorage raftStorage = func.apply(directory);
        test(raftStorage, () -> {
            CommandEntry c1 = commandEntry(1, "val1");
            CommandEntry c2 = commandEntry(1, "val2");
            CommandEntry c3 = commandEntry(1, "val3");

            raftStorage.append(c1);
            raftStorage.append(c2);
            raftStorage.append(c3);

            assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(3);
        });

    }

    @ParameterizedTest
    @MethodSource("storages")
    void appendConfigurationEntry(Function<Path, RaftStorage> func, @TempDir Path directory) {
        RaftStorage raftStorage = func.apply(directory);
        test(raftStorage, () -> {
            ConfigurationEntry c1 = configurationEntry(1, 7000);
            ConfigurationEntry c2 = configurationEntry(1, 7000, 7001);
            ConfigurationEntry c3 = configurationEntry(1);

            raftStorage.append(c1);
            raftStorage.append(c2);
            raftStorage.append(c3);

            assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(3);
        });
    }

    @ParameterizedTest
    @MethodSource("storages")
    void reader(Function<Path, RaftStorage> func, @TempDir Path directory) {
        RaftStorage raftStorage = func.apply(directory);
        test(raftStorage, () -> {
            // given
            long timestamp = System.currentTimeMillis();
            CommandEntry c1 = commandEntry(1, timestamp, "val1");
            ConfigurationEntry c2 = configurationEntry(1, timestamp + 10, 7000, 7001);
            raftStorage.append(c1);
            raftStorage.append(c2);

            // when
            LogStorageReader logStorageReader = raftStorage.openReader();
            assertThat(logStorageReader.getCurrentIndex()).isEqualTo(1);
            assertThat(logStorageReader.hasNext()).isTrue();
            assertThat(logStorageReader.next())
                    .isEqualTo(new IndexedLogEntry(commandEntry(1, timestamp, "val1"), 1, 17));
            assertThat(logStorageReader.hasNext()).isTrue();
//        assertThat(logStorageReader.getCurrentIndex()).isEqualTo(2);

            assertThat(logStorageReader.next())
                    .isEqualTo(new IndexedLogEntry(configurationEntry(1, timestamp + 10, 7000, 7001), 2, 21));
            assertThat(logStorageReader.hasNext()).isFalse();
        });

    }

    @ParameterizedTest
    @MethodSource("storages")
    void readerInitializedBeforeAppend(Function<Path, RaftStorage> func, @TempDir Path directory) {
        RaftStorage raftStorage = func.apply(directory);
        test(raftStorage, () -> {
            // given
            LogStorageReader logStorageReader = raftStorage.openReader();

            long timestamp = System.currentTimeMillis();
            CommandEntry c1 = commandEntry(1, timestamp, "val1");
            ConfigurationEntry c2 = configurationEntry(1, timestamp + 10, 7000, 7001);
            raftStorage.append(c1);
            raftStorage.append(c2);

            // when
            assertThat(logStorageReader.getCurrentIndex()).isEqualTo(1);
            assertThat(logStorageReader.hasNext()).isTrue();
            assertThat(logStorageReader.next())
                    .isEqualTo(new IndexedLogEntry(commandEntry(1, timestamp, "val1"), 1, 17));
            assertThat(logStorageReader.hasNext()).isTrue();
//        assertThat(logStorageReader.getCurrentIndex()).isEqualTo(2);

            assertThat(logStorageReader.next())
                    .isEqualTo(new IndexedLogEntry(configurationEntry(1, timestamp + 10, 7000, 7001), 2, 21));
            assertThat(logStorageReader.hasNext()).isFalse();
        });
    }

    @ParameterizedTest
    @MethodSource("storages")
    void boundedReader(Function<Path, RaftStorage> func, @TempDir Path directory) {
        RaftStorage raftStorage = func.apply(directory);
        test(raftStorage, () -> {
            // given
            long timestamp = System.currentTimeMillis();
            CommandEntry c1 = commandEntry(1, timestamp, "val1");
            ConfigurationEntry c2 = configurationEntry(1, timestamp + 10, 7000, 7001);
            raftStorage.append(c1);
            raftStorage.append(c2);

            // when
            LogStorageReader logStorageReader = raftStorage.openReader();
            BoundedLogStorageReader boundedLogStorageReader = new BoundedLogStorageReader(logStorageReader);

            assertThat(boundedLogStorageReader.getEntriesByIndex(1, 2))
                    .containsExactly(
                            new IndexedLogEntry(commandEntry(1, timestamp, "val1"), 1, 17),
                            new IndexedLogEntry(configurationEntry(1, timestamp + 10, 7000, 7001), 2, 21)
                    );
        });
    }

    private CommandEntry commandEntry(int term, String value) {
        return commandEntry(term, System.currentTimeMillis(), value);
    }

    private CommandEntry commandEntry(int term, long timestamp, String value) {
        return new CommandEntry(term, timestamp, value.getBytes());
    }

    private ConfigurationEntry configurationEntry(int term, int... members) {
        return configurationEntry(term, System.currentTimeMillis(), members);
    }

    private ConfigurationEntry configurationEntry(int term, long timestamp, int... members) {
        Set<Integer> setMembers = new HashSet<>();
        for (int i = 0; i < members.length; i++) {
            setMembers.add(members[i]);
        }
        return new ConfigurationEntry(term, timestamp, setMembers);
    }

    private void test(RaftStorage raftStorage, Runnable runnable) {
        try {
            runnable.run();
        } finally {
            raftStorage.close();
        }
    }
}