package io.github.zomky.utils;

public class Preconditions {

    public static void checkState(boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

}
