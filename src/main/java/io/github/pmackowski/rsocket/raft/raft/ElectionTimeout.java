package io.github.pmackowski.rsocket.raft.raft;

import java.time.Duration;
import java.util.Random;

public class ElectionTimeout {

    public static final int ELECTION_TIMEOUT_MIN_IN_MILLIS = 150;
    private static final Random RANDOM = new Random();

    // [ELECTION_TIMEOUT_MIN_IN_MILLIS, 2*ELECTION_TIMEOUT_MIN_IN_MILLIS)
    public Duration nextRandom() {
        return Duration
                .ofMillis(ELECTION_TIMEOUT_MIN_IN_MILLIS)
                .plusMillis(RANDOM.nextInt(ELECTION_TIMEOUT_MIN_IN_MILLIS));
    }

}
