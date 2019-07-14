package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;
import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.serialize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SenderConfirmOperatorTest {
    private static final int TEN_MESSAGES = 10;

    @Spy
    private DefaultRaftServer node;

    @Spy
    private RaftGroup raftGroup;

    @Mock
    private RaftStorage raftStorage;

    AtomicLong index;

    @BeforeEach
    public void setUp() {
        index = new AtomicLong();
        raftGroup.raftStorage = raftStorage;
        Mockito.lenient().doReturn(2).when(raftGroup).quorum();
        Mockito.lenient().when(raftStorage.append(any(ByteBuffer.class))).thenAnswer(invocation -> {
            long idx = index.incrementAndGet();
            ByteBuffer logEntry = (ByteBuffer) invocation.getArguments()[0];
            return new IndexedLogEntry(deserialize(logEntry), idx, 0);
        });
    }

    @Test
    public void emptyStream() {
        Flux<Payload> payloads = Flux.empty();

        StepVerifier.create(new SenderConfirmOperator(payloads, node, raftGroup, raftStorage))
                .expectSubscription()
                .verifyComplete();

        verify(raftGroup).addConfirmListener(any());

    }

    @Test
    public void storageFailure() {
        Flux<Payload> payloads = payloads(TEN_MESSAGES);

        when(raftStorage.append(any(ByteBuffer.class))).thenThrow(new RuntimeException("append log failed"));

        StepVerifier.create(new SenderConfirmOperator(payloads, node, raftGroup, raftStorage))
                .expectSubscription()
                .verifyErrorMessage("append log failed");

        verify(raftGroup).addConfirmListener(any());

    }

    @Test
    public void requestAndCancel() {
        Flux<Payload> payloads = payloads(TEN_MESSAGES);

        StepVerifier.withVirtualTime(() -> new SenderConfirmOperator(payloads, node, raftGroup, raftStorage), 2)
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(10))
            .then(() -> {
                verify(raftStorage, Mockito.times(2)).append(any(ByteBuffer.class));
                raftGroup.setCommitIndex(2);
            })
            .expectNextCount(2)
            .thenCancel()
            .verify();

        verify(raftGroup).addConfirmListener(any());
    }

    @Test
    public void requestInBatches() {
        Flux<Payload> payloads = payloads(TEN_MESSAGES);

        StepVerifier.withVirtualTime(() -> new SenderConfirmOperator(payloads, node, raftGroup, raftStorage), 2)
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(10))
            .then(() -> {
                verify(raftStorage, Mockito.times(2))
                        .append( any(ByteBuffer.class));
                raftGroup.setCommitIndex(2);
            })
            .expectNextCount(2)
            .thenRequest(8)
            .expectNoEvent(Duration.ofSeconds(10))
            .then(() -> {
                verify(raftStorage, Mockito.times(10))
                        .append(any(ByteBuffer.class));
                raftGroup.setCommitIndex(10);
            })
            .expectNextCount(8)
            .verifyComplete();

        verify(raftGroup).addConfirmListener(any());

    }

    private Flux<Payload> payloads(int nbMessages) {
        return Flux.range(1,nbMessages).map(i -> {
            LogEntry logEntry = new CommandEntry(i, i, ("abc"+i).getBytes());
            ByteBuffer buffer = ByteBuffer.allocate(LogEntry.SIZE + ("abc"+i).length() + 1);
            serialize(logEntry, buffer);
            buffer.flip();
            return DefaultPayload.create(buffer);
        });
    }

}