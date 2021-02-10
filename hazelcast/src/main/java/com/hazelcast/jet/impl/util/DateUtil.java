package com.hazelcast.jet.impl.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public final class DateUtil {

    private static final DateTimeFormatter LOCAL_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private DateUtil() {
    }

    private static ZonedDateTime toZonedDateTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
    }

    public static LocalDateTime toLocalDateTime(long timestamp) {
        return toZonedDateTime(timestamp).toLocalDateTime();
    }

    public static String toLocalTime(long timestamp) {
        return toZonedDateTime(timestamp).toLocalTime().format(LOCAL_TIME_FORMATTER);
    }
}
