package io.github.pmackowski.rsocket.raft.raft;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ElectionTimeoutTest {

    @Test
    void electionTimeoutExactly() {
        ElectionTimeout electionTimeout = ElectionTimeout.exactly(150);

        assertThat(electionTimeout.nextRandom()).isEqualTo(Duration.ofMillis(150));
    }

    @Test
    void electionTimeoutExactlyWithDuration() {
        ElectionTimeout electionTimeout = ElectionTimeout.exactly(Duration.ofSeconds(1));

        assertThat(electionTimeout.nextRandom()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void electionTimeoutRandom() {
        ElectionTimeout electionTimeout = ElectionTimeout.random(150);

        Duration actual = electionTimeout.nextRandom();

        assertThat(actual).isGreaterThanOrEqualTo(Duration.ofMillis(150));
        assertThat(actual).isLessThanOrEqualTo(Duration.ofMillis(2 * 150));
    }

    @Test
    void electionTimeoutRandomWithDuration() {
        ElectionTimeout electionTimeout = ElectionTimeout.random(Duration.ofSeconds(1));

        Duration actual = electionTimeout.nextRandom();

        assertThat(actual).isGreaterThanOrEqualTo(Duration.ofSeconds(1));
        assertThat(actual).isLessThanOrEqualTo(Duration.ofSeconds(2));
    }

    @Test
    void electionTimeoutBetween() {
        ElectionTimeout electionTimeout = ElectionTimeout.between(150, 170);

        Duration actual = electionTimeout.nextRandom();

        assertThat(actual).isGreaterThanOrEqualTo(Duration.ofMillis(150));
        assertThat(actual).isLessThanOrEqualTo(Duration.ofMillis(170));
    }

    @Test
    void electionTimeoutBetweenWithDuration() {
        ElectionTimeout electionTimeout = ElectionTimeout.between(Duration.ofSeconds(1), Duration.ofSeconds(2));

        Duration actual = electionTimeout.nextRandom();

        assertThat(actual).isGreaterThanOrEqualTo(Duration.ofSeconds(1));
        assertThat(actual).isLessThanOrEqualTo(Duration.ofSeconds(2));
    }

    @Test
    void electionTimeoutBetweenMax() {
        ElectionTimeout electionTimeout = ElectionTimeout.between(150, 170);

        Duration actual = electionTimeout.max();

        assertThat(actual).isEqualTo(Duration.ofMillis(170));
    }
}