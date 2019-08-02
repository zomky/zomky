package io.github.pmackowski.rsocket.raft.integration.gossip;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ProbeOperatorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProbeOperatorTest.class);

    @Test
    public void takeAll() throws InterruptedException {

        Mono<Integer> source = Mono.just(1).delayElement(Duration.ofMillis(200));
        Flux<Integer> fallback = Flux.just(3, 4);

        final ProbeOperator<Integer, List<Integer>,Long,Long> flux = new ProbeOperator<>(source, fallback, Mono.delay(Duration.ofMillis(100)), Mono.delay(Duration.ofSeconds(1)));

        flux.subscribe(i -> LOGGER.info("{}", i));

        Thread.sleep(2_000);
        StepVerifier.create(flux)
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(1000))
                .expectNext(Arrays.asList(3,4,1))
                .expectComplete()
                .verify();
    }

    @Test
    public void takeAlsl() {

        Mono<Integer> direct = Mono.just(1);
        Flux<Integer> indirect = Flux.just(3, 4);

        final ProbeOperator<Integer, List<Integer>,Long,Long> flux = new ProbeOperator<>(direct, indirect, Mono.delay(Duration.ofMillis(100)), Mono.delay(Duration.ofSeconds(1)));

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(1))
                .expectNext(Collections.singletonList(1))
                .expectComplete()
                .verify();
    }

}
