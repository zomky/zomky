package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 0, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 2000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
public class RaftStorageGetEntryByIndex {

    FileSystemRaftStorage raftStorage;

    Path directory;

    LogEntry logEntry;

    @Param({"32", "256"})
    public int messageSize;

    @Param({"1000", "100000", "1000000", "10000000"})
    public int numberOfMessages;

    private int segmentSize = 32; // 32MB

    @Setup
    public void setup() throws Exception {
        directory = RaftStorageBenchmarkUtils.createTempDirectory();
        raftStorage = new FileSystemRaftStorage(RaftStorageConfiguration.builder()
                .segmentSize(SizeUnit.megabytes, segmentSize)
                .directory(Paths.get(directory.toAbsolutePath().toString()))
                .build());
        logEntry = RaftStorageBenchmarkUtils.commandEntry(1, 1, messageSize);
        IntStream.rangeClosed(1, numberOfMessages).forEach(i -> raftStorage.append(logEntry));
    }

    @TearDown
    public void tearDown() throws IOException {
        raftStorage.close();
        RaftStorageBenchmarkUtils.delete(directory);
    }

    @Benchmark
    public void getEntryByIndex(Blackhole blackhole) {
        blackhole.consume(raftStorage.getEntryByIndex(RaftStorageBenchmarkUtils.random(numberOfMessages)));
    }

    @Benchmark
    @Threads(4)
    public void getEntryByIndexConcurrent(Blackhole blackhole) {
        blackhole.consume(raftStorage.getEntryByIndex(RaftStorageBenchmarkUtils.random(numberOfMessages)));
    }

}
