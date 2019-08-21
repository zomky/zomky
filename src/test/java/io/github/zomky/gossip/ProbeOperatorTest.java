package io.github.zomky.gossip;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ProbeOperatorTest {

    @Test
    void successfulDirectWithinRoundTripTime() {
        Mono<Integer> direct = Mono.just(1);
        Flux<Integer> indirect = Flux.just(2, 3);
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> probeTimeout = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, probeTimeout))
                    .expectSubscription()
                    .thenAwait(Duration.ofMillis(50))
                    .assertNext(o -> {
                        ProbeOperatorResult<Integer> probeOperatorResult = (ProbeOperatorResult<Integer>) o;
                        assertThat(probeOperatorResult.isIndirect()).isFalse();
                        assertThat(probeOperatorResult.isDirectSuccessful()).isTrue();
                        assertThat(probeOperatorResult.getElements()).containsExactly(1);
                    })
                    .expectComplete()
                    .verify();
    }

    @Test
    void successfulDirectAfterRoundTripTime() {
        Mono<Integer> direct = Mono.just(1).delayElement(Duration.ofMillis(40));
        Flux<Integer> indirect = Flux.just(2, 3);
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> probeTimeout = Mono.delay(Duration.ofMillis(100));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, probeTimeout))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(50))
                .assertNext(o -> {
                    ProbeOperatorResult<Integer> probeOperatorResult = (ProbeOperatorResult<Integer>) o;
                    assertThat(probeOperatorResult.isIndirect()).isTrue();
                    assertThat(probeOperatorResult.isDirectSuccessful()).isTrue();
                    assertThat(probeOperatorResult.getElements()).containsExactly(2,3,1);
                })
                .expectComplete()
                .verify();
    }

    @Test
    void successfulDirectAfterRoundTripTimeAndIndirectFailed() {
        Mono<Integer> direct = Mono.just(1).delayElement(Duration.ofMillis(30));
        Flux<Integer> indirect = Flux.error(new RuntimeException());
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> probeTimeout = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, probeTimeout))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(50))
                .assertNext(o -> {
                    ProbeOperatorResult<Integer> probeOperatorResult = (ProbeOperatorResult<Integer>) o;
                    assertThat(probeOperatorResult.isIndirect()).isTrue();
                    assertThat(probeOperatorResult.isDirectSuccessful()).isTrue();
                    assertThat(probeOperatorResult.getElements()).containsExactly(1);
                })
                .expectComplete()
                .verify();
    }

    @Test
    void successfulDirectAfterRoundTripTimeAndIndirectPartiallySuccessful() {
        Mono<Integer> direct = Mono.just(1).delayElement(Duration.ofMillis(30));
        Flux<Integer> indirect = Flux.mergeDelayError(2, Mono.error(new RuntimeException()), Mono.just(2));
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> probeTimeout = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, probeTimeout))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(50))
                .assertNext(o -> {
                    ProbeOperatorResult<Integer> probeOperatorResult = (ProbeOperatorResult<Integer>) o;
                    assertThat(probeOperatorResult.isIndirect()).isTrue();
                    assertThat(probeOperatorResult.isDirectSuccessful()).isTrue();
                    assertThat(probeOperatorResult.getElements()).containsExactly(2,1);
                })
                .expectComplete()
                .verify();
    }

    @Test
    void successfulDirectAfterRoundTripTimeAndIndirectPartiallySuccessful2() {
        Mono<Integer> direct = Mono.just(1).delayElement(Duration.ofMillis(30));
        Flux<Integer> indirect = Flux.mergeDelayError(2, Mono.error(new RuntimeException()), Mono.just(2).delayElement(Duration.ofMillis(50)));
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> probeTimeout = Mono.delay(Duration.ofMillis(100));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, probeTimeout))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(100))
                .assertNext(o -> {
                    ProbeOperatorResult<Integer> probeOperatorResult = (ProbeOperatorResult<Integer>) o;
                    assertThat(probeOperatorResult.isIndirect()).isTrue();
                    assertThat(probeOperatorResult.isDirectSuccessful()).isTrue();
                    assertThat(probeOperatorResult.getElements()).containsExactly(1,2);
                })
                .expectComplete()
                .verify();
    }

    @Test
    void successfulDirectAfterProbeTimeout() {
        Mono<Integer> direct = Mono.just(1).delayElement(Duration.ofMillis(100));
        Flux<Integer> indirect = Flux.just(2, 3);
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> probeTimeout = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, probeTimeout))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(50))
                .assertNext(o -> {
                    ProbeOperatorResult<Integer> probeOperatorResult = (ProbeOperatorResult<Integer>) o;
                    assertThat(probeOperatorResult.isIndirect()).isTrue();
                    assertThat(probeOperatorResult.isDirectSuccessful()).isFalse();
                    assertThat(probeOperatorResult.getElements()).containsExactly(2,3);
                })
                .expectComplete()
                .verify();
    }

    @Test
    void failedDirect() {
        Mono<Integer> direct = Mono.error(new RuntimeException());
        Flux<Integer> indirect = Flux.just(1, 2);
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> probeTimeout = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, probeTimeout))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(50))
                .assertNext(o -> {
                    ProbeOperatorResult<Integer> probeOperatorResult = (ProbeOperatorResult<Integer>) o;
                    assertThat(probeOperatorResult.isIndirect()).isTrue();
                    assertThat(probeOperatorResult.isDirectSuccessful()).isFalse();
                    assertThat(probeOperatorResult.getElements()).containsExactly(1,2);
                })
                .expectComplete()
                .verify();
    }

    @Test
    void failedBothDirectAndIndirect() {
        Mono<Integer> direct = Mono.error(new RuntimeException());
        Flux<Integer> indirect = Flux.error(new RuntimeException());
        Mono<Long> indirectStart = Mono.delay(Duration.ofMillis(10));
        Mono<Long> probeTimeout = Mono.delay(Duration.ofMillis(50));

        StepVerifier.create(new ProbeOperator<>(direct, indirect, indirectStart, probeTimeout))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(50))
                .assertNext(o -> {
                    ProbeOperatorResult<Integer> probeOperatorResult = (ProbeOperatorResult<Integer>) o;
                    assertThat(probeOperatorResult.isIndirect()).isTrue();
                    assertThat(probeOperatorResult.isDirectSuccessful()).isFalse();
                    assertThat(probeOperatorResult.getElements()).isEmpty();
                })
                .expectComplete()
                .verify();
    }

}
