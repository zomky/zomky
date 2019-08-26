package io.github.zomky.gossip;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class SuspectTimerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SuspectTimerTest.class);

    @DisplayName("Suspicion timeout")
    @ParameterizedTest(name = "Suspicion timeout should be {5} for cluster size {0}, lambda {1}, beta {2}, independentSuspicions {3}, parameter k {4}")
    @CsvSource({
            //      cluster size  ,lambda   ,beta     ,independentSuspicions   ,parameter k   ,expected timeout
            "1            ,2        ,2        ,0                       ,3             ,0               ",
            "1            ,2        ,2        ,3                       ,3             ,0               ",

            "2            ,2        ,2        ,0                       ,3             ,1204            ",
            "2            ,2        ,2        ,3                       ,3             ,602             ",
            "2            ,2        ,4        ,0                       ,3             ,2408            ",
            "2            ,2        ,4        ,3                       ,3             ,602             ",
            "2            ,5        ,6        ,0                       ,3             ,9030            ",
            "2            ,5        ,6        ,3                       ,3             ,1505            ",

            "3            ,2        ,2        ,0                       ,3             ,1908            ",
            "3            ,2        ,2        ,3                       ,3             ,954             ",
            "3            ,2        ,4        ,0                       ,3             ,3816            ",
            "3            ,2        ,4        ,3                       ,3             ,954             ",
            "3            ,5        ,6        ,0                       ,3             ,14316           ",
            "3            ,5        ,6        ,3                       ,3             ,2386            ",

            "100          ,2        ,2        ,0                       ,3             ,8000            ",
            "100          ,2        ,2        ,3                       ,3             ,4000            ",
            "100          ,2        ,4        ,0                       ,3             ,16000           ",
            "100          ,2        ,4        ,3                       ,3             ,4000            ",
            "100          ,5        ,6        ,0                       ,3             ,60000           ",
            "100          ,5        ,6        ,1                       ,3             ,35000           ",
            "100          ,5        ,6        ,2                       ,3             ,20376           ",
            "100          ,5        ,6        ,3                       ,3             ,10000           "
    })
        // probe interval is 1000ms
    void suspicionTimeout(int clusterSize, int lambda, int beta, int independentSuspicions, int k, long expectedSuspicionTimeout) {
        // given
        Duration probeInterval = Duration.ofSeconds(1);
        SuspectTimer suspectTimer = new SuspectTimer();
        suspectTimer.setIndependentSuspicions(independentSuspicions);
        suspectTimer.setClusterSize(clusterSize);
        suspectTimer.setProbeInterval(probeInterval);

        // when
        long actual = suspectTimer.suspicionTimeout(lambda, beta, k);

        // then
        assertThat(actual).isEqualTo(expectedSuspicionTimeout);
    }

}