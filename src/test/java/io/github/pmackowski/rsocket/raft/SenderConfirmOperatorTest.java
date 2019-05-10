package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.LogEntryInfo;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SenderConfirmOperatorTest {

    private static final int TEN_MESSAGES = 10;

    @Spy
    private DefaultRaftServer node;

    @Mock
    private RaftStorage raftStorage;

    AtomicLong index;

    @BeforeEach
    public void setUp() {
        index = new AtomicLong();

        Mockito.lenient().when(raftStorage.appendLog(anyInt(), any())).thenAnswer(invocation -> {
            long idx = index.incrementAndGet();
            int term = (int) invocation.getArguments()[0];
            return new LogEntryInfo().term(term).index(idx);
        });
    }

    @Test
    public void emptyStream() {
        Flux<Payload> payloads = Flux.empty();

        StepVerifier.create(new SenderConfirmOperator(payloads, node, raftStorage))
                .expectSubscription()
                .verifyComplete();

        verify(node).addConfirmListener(any());

    }

    @Test
    public void storageFailure() {
        Flux<Payload> payloads = payloads(TEN_MESSAGES);

        when(raftStorage.appendLog(anyInt(), any())).thenThrow(new RuntimeException("append log failed"));

        StepVerifier.create(new SenderConfirmOperator(payloads, node, raftStorage))
                .expectSubscription()
                .verifyErrorMessage("append log failed");

        verify(node).addConfirmListener(any());

    }

    @Test
    public void requestAndCancel() {
        Flux<Payload> payloads = payloads(TEN_MESSAGES);

        StepVerifier.withVirtualTime(() -> new SenderConfirmOperator(payloads, node, raftStorage), 2)
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(10))
            .then(() -> {
                verify(raftStorage, Mockito.times(2)).appendLog(anyInt(), any());
                node.setCommitIndex(2);
            })
            .expectNextCount(2)
            .thenCancel()
            .verify();

        verify(node).addConfirmListener(any());
    }

    @Test
    public void requestInBatches() {
        Flux<Payload> payloads = payloads(TEN_MESSAGES);

        StepVerifier.withVirtualTime(() -> new SenderConfirmOperator(payloads, node, raftStorage), 2)
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(10))
            .then(() -> {
                verify(raftStorage, Mockito.times(2))
                        .appendLog(anyInt(), any());
                node.setCommitIndex(2);
            })
            .expectNextCount(2)
            .thenRequest(8)
            .expectNoEvent(Duration.ofSeconds(10))
            .then(() -> {
                verify(raftStorage, Mockito.times(10))
                        .appendLog(anyInt(), any());
                node.setCommitIndex(10);
            })
            .expectNextCount(8)
            .verifyComplete();

        verify(node).addConfirmListener(any());

    }

    private Flux<Payload> payloads(int nbMessages) {
        return Flux.range(1,nbMessages).map(i -> DefaultPayload.create("m"+i));
    }
}