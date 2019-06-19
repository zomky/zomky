package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 0, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 1, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
public class RaftStorageGetEntryByIndex {

    private static final Random RANDOM = new Random();

    RaftStorage raftStorage;

    Path directory;

    LogEntry logEntry;

//    @Param({"32", "256"})
    @Param({"32"})
    public int messageSize;

//    @Param({"1000", "100000", "1000000", "10000000"})
    @Param({"1000000"})
    public int numberOfMessages;

    private int segmentSize = 32; // 32MB

    @Setup
    public void setupConnection() throws Exception {
        directory = RaftStorageBenchmarkUtils.createTempDirectory();
        raftStorage = new RaftStorage(RaftStorageConfiguration.builder()
                .segmentSize(SizeUnit.megabytes, segmentSize)
                .directory(Paths.get(directory.toAbsolutePath().toString()))
                .build());
        logEntry = RaftStorageBenchmarkUtils.commandEntry(1, 1, messageSize);
        IntStream.rangeClosed(1, numberOfMessages).forEach(i -> raftStorage.append(logEntry));
    }

    @TearDown
    public void closeConnection() throws IOException {
        raftStorage.close();
        RaftStorageBenchmarkUtils.delete(directory);
    }

    @Benchmark
    public void getEntryByIndex(Blackhole blackhole) {
        blackhole.consume(raftStorage.getEntryByIndex(random(numberOfMessages)));
    }

    @Benchmark
    @Threads(4)
    public void getEntryByIndexConcurrent(Blackhole blackhole) {
        blackhole.consume(raftStorage.getEntryByIndex(random(numberOfMessages)));

    }
//    TODO investigate difference in performance
//    Benchmark                                             (messageSize)  (numberOfMessages)   Mode  Cnt      Score   Error  Units
//    RaftStorageGetEntryByIndex.getEntryByIndex                       32             1000000  thrpt       48340,376          ops/s
//    RaftStorageGetEntryByIndex.getEntryByIndexConcurrent             32             1000000  thrpt         370,122          ops/s

    private int random(int max) {
        return 1 + RANDOM.nextInt(max);
//        return 1 + ThreadLocalRandom.current().nextInt(max);
    }

}
