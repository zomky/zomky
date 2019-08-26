package io.github.zomky.gossip;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

import java.time.Duration;

class SuspectTimersTest {

    private static int K_0_TIMEOUT = 192;
    private static int K_1_TIMEOUT = 115;
    private static int K_2_TIMEOUT = 72;
    private static int K_3_TIMEOUT = 42;

    SuspectTimers suspectTimers;

    @BeforeEach
    void setUp() {
        suspectTimers = SuspectTimers.builder()
                .suspicionMinMultiplier(2)
                .suspicionMaxMultiplier(4)
                .k(3)
                .build();
        // timeouts for 'k':
        // 0 - 192ms
        // 1 - 115ms (due to elapsed time might be a little less)
        // 2 - 72ms  (due to elapsed time might be a little less)
        // 3 - 42ms  (due to elapsed time might be a little less)
    }

    @Test
    void initializeTimer() {
        StepVerifier.create(suspectTimers.deadNodes().log())
                .then(() -> suspectTimers.initializeTimer(7001, Duration.ofMillis(50), 3))
                .expectNoEvent(Duration.ofMillis(K_0_TIMEOUT))
                .expectNext(7001)
                .thenCancel()
                .verify();
    }

    @Test
    void initializeAndThenRemoveTimer() {
        StepVerifier.create(suspectTimers.deadNodes().log())
                .then(() -> suspectTimers.initializeTimer(7001, Duration.ofMillis(50), 3))
                .thenAwait(Duration.ofMillis(K_0_TIMEOUT - 50))
                .then(() -> suspectTimers.removeTimer(7001))
                .expectNoEvent(Duration.ofMillis(50))
                .thenCancel()
                .verify();
    }

    @Test
    void incrementIndependentSuspicion() {
        StepVerifier.create(suspectTimers.deadNodes().log())
                .then(() -> suspectTimers.initializeTimer(7001, Duration.ofMillis(50), 3))
                .then(() -> suspectTimers.incrementIndependentSuspicion(7001))
                .then(() -> suspectTimers.incrementIndependentSuspicion(7001))
                .then(() -> suspectTimers.incrementIndependentSuspicion(7001))
                .expectNoEvent(Duration.ofMillis(40))
                .expectNext(7001)
                .thenCancel()
                .verify();
    }
}