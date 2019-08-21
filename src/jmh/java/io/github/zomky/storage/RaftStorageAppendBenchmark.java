package io.github.zomky.storage;

import io.github.zomky.storage.log.SizeUnit;
import io.github.zomky.storage.log.entry.LogEntry;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 0, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 2, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
public class RaftStorageAppendBenchmark {

    FileSystemRaftStorage raftStorage;

    Path directory;

    LogEntry logEntry;

    @Param({"32", "256", "1024", "1048576"})
    public int messageSize;

    @Param({"2", "8", "32", "64"})
    public int segmentSize;

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        directory = RaftStorageBenchmarkUtils.createTempDirectory();
        raftStorage = new FileSystemRaftStorage(RaftStorageConfiguration.builder()
                .segmentSize(SizeUnit.megabytes, segmentSize)
                .directory(Paths.get(directory.toAbsolutePath().toString()))
                .build());
        logEntry = RaftStorageBenchmarkUtils.commandEntry(1, 1, messageSize);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() throws Exception {
        raftStorage.close();
        RaftStorageBenchmarkUtils.delete(directory);
    }

    @Benchmark
    public void appendLog(Blackhole blackhole) {
        blackhole.consume(raftStorage.append(logEntry));
    }

    @Benchmark
    @Threads(2)
    public void appendLogConcurrent(Blackhole blackhole) {
        blackhole.consume(raftStorage.append(logEntry));
    }

}
