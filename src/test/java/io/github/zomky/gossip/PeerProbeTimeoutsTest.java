package io.github.zomky.gossip;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class PeerProbeTimeoutsTest {

    @DisplayName("probe timeout")
    @ParameterizedTest(name = "Probe timeout should be {2} for base probe timeout {0} and LHM {1}")
    @CsvSource({
            "1000, 0, 1000", // healthy node
            "1000, 1, 2000",
            "1000, 8, 9000", // unhealthy node

            "3000, 0, 3000", // healthy node
            "3000, 1, 6000",
            "3000, 8, 27000" // unhealthy node
    })
    void probeTimeout(int baseProbeTimeout, int localHealthMultiplier, int expectedProbeTimeout) {
        // given
        PeerProbeTimeouts probeTimeouts = PeerProbeTimeouts.builder()
            .baseProbeTimeout(Duration.ofMillis(baseProbeTimeout))
            .localHealthMultiplier(localHealthMultiplier)
            .build();

        // when
        Duration actual = probeTimeouts.probeTimeout();

        // then
        assertThat(actual).isEqualTo(Duration.ofMillis(expectedProbeTimeout));
    }

    @DisplayName("indirect delay")
    @ParameterizedTest(name = "Indirect delay should be {2} for LHM {0} and indirect delay ratio {1}")
    @CsvSource({ // base probe timeout = 1000ms
            "0, 0.3f, 300", // healthy node
            "0, 0.4f, 400",
            "1, 0.3f, 600",
            "1, 0.4f, 800",
            "8, 0.3f, 2700" // unhealthy node
    })
    void indirectDelay(int localHealthMultiplier, float indirectDelayRatio, int expectedIndirectDelay) {
        // given
        PeerProbeTimeouts probeTimeouts = PeerProbeTimeouts.builder()
                .baseProbeTimeout(Duration.ofSeconds(1))
                .localHealthMultiplier(localHealthMultiplier)
                .indirectDelayRatio(indirectDelayRatio)
                .build();

        // when
        Duration actual = probeTimeouts.indirectDelay();

        // then
        assertThat(actual).isEqualTo(Duration.ofMillis(expectedIndirectDelay));
    }

    @DisplayName("nack timeout")
    @ParameterizedTest(name = "Nack timeout should be {3} for LHM {0}, indirect delay ratio {1} and nack ratio {2}")
    @CsvSource({ // base probe timeout = 1000ms
            "0, 0.3f, 0.5f, 350", // healthy node
            "0, 0.4f, 0.5f, 300",
            "0, 0.3f, 0.6f, 420",
            "0, 0.4f, 0.6f, 360",
            "1, 0.3f, 0.5f, 700",
            "1, 0.4f, 0.5f, 600",
            "8, 0.3f, 0.5f, 3150" // unhealthy node
    })
    void nackTimeout(int localHealthMultiplier, float indirectDelayRatio, float nackRatio, int expectedNackTimeout) {
        // given
        PeerProbeTimeouts probeTimeouts = PeerProbeTimeouts.builder()
                .baseProbeTimeout(Duration.ofSeconds(1))
                .localHealthMultiplier(localHealthMultiplier)
                .indirectDelayRatio(indirectDelayRatio)
                .nackRatio(nackRatio)
                .build();

        // when
        Duration actual = probeTimeouts.nackTimeout();

        // then
        assertThat(actual).isEqualTo(Duration.ofMillis(expectedNackTimeout));
    }
}