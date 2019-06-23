package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
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
@Measurement(iterations = 2, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
public class RaftStorageChunkReaderBenchmark {

    FileSystemRaftStorage raftStorage;

    Path directory;

    LogEntry logEntry;

    LogStorageReader logStorageReader;

    @Param({"32", "256", "1024"})
    public int messageSize;

    int segmentSize = 32;
    int numberOfMessages = 20_000_000;

    @Setup
    public void setup() throws Exception {
        directory = RaftStorageBenchmarkUtils.createTempDirectory();
        System.out.println(directory);
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

    @Setup(Level.Iteration)
    public void setupIteration() {
        logStorageReader = raftStorage.openReader();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        logStorageReader.close();
    }

    @Benchmark
    public void iterate(Blackhole blackhole) {
        blackhole.consume(logStorageReader.next());
    }

    @Benchmark
    public void reset() {
        logStorageReader.reset(RaftStorageBenchmarkUtils.random(numberOfMessages));
    }

}
