package io.github.pmackowski.rsocket.raft.gossip;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class LocalHealthMultiplierTest {

    @Test
    void inc() {
        // given
        LocalHealthMultiplier localHealthMultiplier = new LocalHealthMultiplier(10);

        // when
        int actual = localHealthMultiplier.inc();

        // then
        assertThat(actual).isEqualTo(1);
    }

    @Test
    void incOverflow() {
        // given
        LocalHealthMultiplier localHealthMultiplier = new LocalHealthMultiplier(10);

        // when
        IntStream.rangeClosed(1,100).forEach(i -> localHealthMultiplier.inc());

        // then
        assertThat(localHealthMultiplier.value()).isEqualTo(10);
    }

    @Test
    void dec() {
        // given
        LocalHealthMultiplier localHealthMultiplier = new LocalHealthMultiplier(10);
        IntStream.rangeClosed(1,5).forEach(i -> localHealthMultiplier.inc());

        // when
        int actual = localHealthMultiplier.dec();

        // then
        assertThat(actual).isEqualTo(4);
    }

    @Test
    void decOverflow() {
        // given
        LocalHealthMultiplier localHealthMultiplier = new LocalHealthMultiplier(10);
        IntStream.rangeClosed(1,5).forEach(i -> localHealthMultiplier.inc());

        // when
        IntStream.rangeClosed(1,100).forEach(i -> localHealthMultiplier.dec());

        // then
        assertThat(localHealthMultiplier.value()).isEqualTo(0);
    }
}