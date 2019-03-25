package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;
import rsocket.playground.raft.storage.LogEntryInfo;
import rsocket.playground.raft.storage.ZomkyStorage;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SenderConfirmOperatorTest {

    private static final int TEN_MESSAGES = 10;

    @Spy
    private Node node;

    @Mock
    private ZomkyStorage zomkyStorage;

    DirectProcessor<Long> processor;
    FluxSink<Long> sink;
    AtomicLong index;


    @Before
    public void setUp() {
        processor = DirectProcessor.create();
        sink = processor.sink();
        index = new AtomicLong();

        when(zomkyStorage.appendLog(anyInt(), any())).thenAnswer(invocation -> {
            long idx = index.incrementAndGet();
            sink.next(idx);
            int term = (int) invocation.getArguments()[0];
            return new LogEntryInfo().term(term).index(idx);
        });

    }

    @Test
    public void emptyStream() {
        Flux<Payload> payloads = Flux.empty();

        StepVerifier.create(new SenderConfirmOperator(payloads, node, zomkyStorage))
                .expectSubscription()
                .verifyComplete();

        verify(node).addConfirmListener(any());

    }

    @Test
    public void storageFailure() {
        Flux<Payload> payloads = payloads(TEN_MESSAGES);

        when(zomkyStorage.appendLog(anyInt(), any())).thenThrow(new RuntimeException("append log failed"));

        StepVerifier.create(new SenderConfirmOperator(payloads, node, zomkyStorage))
                .expectSubscription()
                .verifyErrorMessage("append log failed");

        verify(node).addConfirmListener(any());

    }

    @Test
    public void requestAndCancel() {
        Flux<Payload> payloads = payloads(TEN_MESSAGES);

        processor
                .filter(currentIndex -> currentIndex % 2 == 0)
                .delayElements(Duration.ofMillis(100))
                .doOnNext(currentIndex -> node.setCommitIndex(currentIndex))
                .subscribe();

        StepVerifier.create(new SenderConfirmOperator(payloads, node, zomkyStorage), 2)
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(100))
                .expectNextCount(2)
                .thenCancel()
                .verify();

        verify(node).addConfirmListener(any());

    }

    @Test
    public void requestInBatches() {
        Flux<Payload> payloads = payloads(TEN_MESSAGES);

        processor
            .filter(currentIndex -> currentIndex % 2 == 0)
            .delayElements(Duration.ofMillis(100))
            .doOnNext(currentIndex -> node.setCommitIndex(currentIndex))
            .subscribe();

        StepVerifier.create(new SenderConfirmOperator(payloads, node, zomkyStorage), 2)
                    .expectSubscription()
                    .expectNoEvent(Duration.ofMillis(100))
                    .expectNextCount(2)
                    .thenRequest(8)
                    .expectNoEvent(Duration.ofMillis(100))
                    .expectNextCount(8)
                    .verifyComplete();

        verify(node).addConfirmListener(any());

    }

    private Flux<Payload> payloads(int nbMessages) {
        return Flux.range(1,nbMessages).map(i -> DefaultPayload.create("m"+i));
    }
}