package io.github.pmackowski.rsocket.raft.storage;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 0, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 1, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
//@Threads(2)
public class DefaultRaftStorageBenchmark {

    DefaultRaftStorage defaultRaftStorage;

    ByteBuffer content;

    @Setup
    public void setupConnection() throws Exception {
        defaultRaftStorage = new DefaultRaftStorage(1112);
    }

    @TearDown
    public void closeConnection() throws Exception {
        defaultRaftStorage.close();
    }

    @Setup(Level.Iteration)
    public void setupSender() throws Exception {
    }

    @TearDown(Level.Iteration)
    public void tearDownSender() throws Exception {
    }

//    @Benchmark
    public void appendLog(Blackhole blackhole) {
        content = ByteBuffer.wrap("--------------------------------".getBytes());
        blackhole.consume(defaultRaftStorage.appendLog(1, content));
    }

    @Benchmark
    public void getEntryByIndex(Blackhole blackhole) {
        content = ByteBuffer.wrap("--------------------------------".getBytes());
        blackhole.consume(defaultRaftStorage.getTermByIndex(100000));
        blackhole.consume(defaultRaftStorage.getEntryByIndex(100000));
    }

    @Benchmark
    public void getEntriesByIndex(Blackhole blackhole) {
        content = ByteBuffer.wrap("--------------------------------".getBytes());
        blackhole.consume(defaultRaftStorage.getEntriesByIndex(100000, 104000));
    }

}
