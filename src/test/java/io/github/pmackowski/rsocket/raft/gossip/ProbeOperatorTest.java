package io.github.pmackowski.rsocket.raft.gossip;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

public class ProbeOperatorTest {

    @Test
    void successfulDirectWithinRoundTripTime() {
        Mono<Integer> direct = Mono.just(1);
        Flux<Integer> indirect = Flux.just(2, 3);
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> protocolPeriodEnd = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, protocolPeriodEnd))
                    .expectSubscription()
                    .expectNoEvent(Duration.ofMillis(40))
                    .expectNext(Collections.singletonList(1))
                    .expectComplete()
                    .verify();
    }

    @Test
    void successfulDirectAfterRoundTripTime() {
        Mono<Integer> direct = Mono.just(1).delayElement(Duration.ofMillis(30));
        Flux<Integer> indirect = Flux.just(2, 3);
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> protocolPeriodEnd = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, protocolPeriodEnd))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(40))
                .expectNext(Arrays.asList(2,3,1))
                .expectComplete()
                .verify();
    }

    @Test
    void successfulDirectAfterRoundTripTimeAndIndirectFailed() {
        Mono<Integer> direct = Mono.just(1).delayElement(Duration.ofMillis(30));
        Flux<Integer> indirect = Flux.error(new RuntimeException());
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> protocolPeriodEnd = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, protocolPeriodEnd))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(40))
                .expectNext(Arrays.asList(1))
                .expectComplete()
                .verify();
    }

    @Test
    void successfulDirectAfterRoundTripTimeAndIndirectPartiallySuccessful() {
        Mono<Integer> direct = Mono.just(1).delayElement(Duration.ofMillis(30));
        Flux<Integer> indirect = Flux.mergeDelayError(2, Mono.error(new RuntimeException()), Mono.just(2));
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> protocolPeriodEnd = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, protocolPeriodEnd))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(40))
                .expectNext(Arrays.asList(2,1))
                .expectComplete()
                .verify();
    }

    @Test
    void successfulDirectAfterRoundTripTimeAndIndirectPartiallySuccessful2() {
        Mono<Integer> direct = Mono.just(1).delayElement(Duration.ofMillis(30));
        Flux<Integer> indirect = Flux.mergeDelayError(2, Mono.error(new RuntimeException()), Mono.just(2).delayElement(Duration.ofMillis(25)));
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> protocolPeriodEnd = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, protocolPeriodEnd))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(40))
                .expectNext(Arrays.asList(1,2))
                .expectComplete()
                .verify();
    }

    @Test
    void successfulDirectAfterProtocolPeriodEnd() {
        Mono<Integer> direct = Mono.just(1).delayElement(Duration.ofMillis(100));
        Flux<Integer> indirect = Flux.just(2, 3);
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> protocolPeriodEnd = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, protocolPeriodEnd))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(40))
                .expectNext(Arrays.asList(2,3))
                .expectComplete()
                .verify();
    }

    @Test
    void failedDirect() {
        Mono<Integer> direct = Mono.error(new RuntimeException());
        Flux<Integer> indirect = Flux.just(1, 2);
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> protocolPeriodEnd = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, protocolPeriodEnd))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(40))
                .expectNext(Arrays.asList(1,2))
                .expectComplete()
                .verify();
    }

    @Test
    void failedBothDirectAndIndirect() {
        Mono<Integer> direct = Mono.error(new RuntimeException());
        Flux<Integer> indirect = Flux.error(new RuntimeException());
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> protocolPeriodEnd = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, protocolPeriodEnd))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(40))
                .expectNext(Collections.emptyList())
                .expectComplete()
                .verify();
    }

}
