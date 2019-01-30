package rsocket.playground.raft;

import java.time.Duration;
import java.util.Random;

class ElectionTimeout {

    private static final int ELECTION_TIMEOUT_MIN_IN_MILLIS = 150;
    private static final Random RANDOM = new Random();

    // [ELECTION_TIMEOUT_MIN_IN_MILLIS, 2*ELECTION_TIMEOUT_MIN_IN_MILLIS)
    static Duration nextRandom() {
        return Duration
                .ofMillis(ELECTION_TIMEOUT_MIN_IN_MILLIS)
                .plusMillis(RANDOM.nextInt(ELECTION_TIMEOUT_MIN_IN_MILLIS));
    }

}
