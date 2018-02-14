package com.hazelcast.internal.util;

import static java.lang.String.format;

/**
 * Utility functions for time related operations
 */
public final class TimeUtil {

    private static final int MILLIS_PER_SECOND = 1000;
    private static final int SECONDS_PER_MINUTE = 60;
    private static final int MINUTES_PER_HOUR = 60;
    private static final int HOURS_PER_DAY = 24;

    private TimeUtil() {
    }

    public static String toHumanReadableMillis(long givenMillis) {
        long time = givenMillis;

        long millis = time % MILLIS_PER_SECOND;
        time /= MILLIS_PER_SECOND;

        long seconds = time % SECONDS_PER_MINUTE;
        time /= SECONDS_PER_MINUTE;

        long minutes = time % MINUTES_PER_HOUR;
        time /= MINUTES_PER_HOUR;

        long hours = time % HOURS_PER_DAY;
        time /= HOURS_PER_DAY;

        long days = time;

        return format("%02dd %02dh %02dm %02ds %02dms", days, hours, minutes, seconds, millis);
    }
}
