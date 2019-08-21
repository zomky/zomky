package io.github.zomky.raft;

import java.time.Duration;
import java.util.Random;

public class ElectionTimeout {

    private static final int DEFAULT_ELECTION_TIMEOUT = 200;


    private static final Random RANDOM = new Random();

    private int minMilliseconds;
    private int maxMilliseconds;

    private ElectionTimeout(int minMilliseconds, int maxMilliseconds) {
        this.minMilliseconds = minMilliseconds;
        this.maxMilliseconds = maxMilliseconds;
    }

    public static ElectionTimeout between(Duration min, Duration max) {
        return between((int) min.toMillis(), (int) max.toMillis());
    }

    public static ElectionTimeout between(int minMilliseconds, int maxMilliseconds) {
        return new ElectionTimeout(minMilliseconds, maxMilliseconds);
    }

    public static ElectionTimeout exactly(Duration duration) {
        return between((int) duration.toMillis(), (int) duration.toMillis());
    }

    public static ElectionTimeout exactly(int milliseconds) {
        return between(milliseconds, milliseconds);
    }

    // [ELECTION_TIMEOUT_MIN_IN_MILLIS, 2*ELECTION_TIMEOUT_MIN_IN_MILLIS)
    public static ElectionTimeout random(int minMilliseconds) {
        return between(minMilliseconds, 2 * minMilliseconds);
    }

    // [ELECTION_TIMEOUT_MIN_IN_MILLIS, 2*ELECTION_TIMEOUT_MIN_IN_MILLIS)
    public static ElectionTimeout random(Duration min) {
        return between((int) min.toMillis(), (int) (2 * min.toMillis()));
    }

    public static ElectionTimeout defaultTimeout() {
        return new ElectionTimeout(DEFAULT_ELECTION_TIMEOUT, DEFAULT_ELECTION_TIMEOUT);
    }

    public Duration nextRandom() {
        if (minMilliseconds == maxMilliseconds) {
            return Duration.ofMillis(minMilliseconds);
        }
        return Duration
                .ofMillis(minMilliseconds)
                .plusMillis(RANDOM.nextInt(maxMilliseconds - minMilliseconds));
    }

    public Duration min() {
        return Duration.ofMillis(minMilliseconds);
    }

    public Duration max() {
        return Duration.ofMillis(maxMilliseconds);
    }

}
