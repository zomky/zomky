package io.github.pmackowski.rsocket.raft.utils;

public class Preconditions {

    public static void checkState(boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

}
