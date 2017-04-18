package com.hazelcast.test.starter;

public class Utils {
    public static final boolean DEBUG_ENABLED = false;

    public static RuntimeException rethrow(Exception e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException)e;
        }
        throw new GuardianException(e);
    }

    public static void debug(String text) {
        if (DEBUG_ENABLED) {
            System.out.println(text);
        }
    }
}
