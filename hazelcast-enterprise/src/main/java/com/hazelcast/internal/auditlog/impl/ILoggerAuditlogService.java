package com.hazelcast.internal.auditlog.impl;

import java.util.Map;

import com.hazelcast.internal.auditlog.AuditableEvent;
import com.hazelcast.internal.auditlog.EventBuilder;
import com.hazelcast.internal.auditlog.AuditlogService;
import com.hazelcast.internal.auditlog.Level;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;

public class ILoggerAuditlogService implements AuditlogService {

    public static final String AUDITLOG_CATEGORY = "hazelcast.auditlog";

    private final ILogger logger;

    public ILoggerAuditlogService(LoggingService loggingService) {
        logger = loggingService != null ? loggingService.getLogger(AUDITLOG_CATEGORY) : Logger.getLogger(AUDITLOG_CATEGORY);
    }

    @Override
    public void log(AuditableEvent auditableEvent) {
        logInternal(auditableEvent.typeId(), auditableEvent.level(), auditableEvent.message(), auditableEvent.parameters(),
                auditableEvent.cause());
    }

    @Override
    public void log(String eventTypeId, Level level, String message) {
        logInternal(eventTypeId, level, message, null, null);
    }

    @Override
    public void log(String eventTypeId, Level level, String message, Throwable thrown) {
        logInternal(eventTypeId, level, message, null, thrown);
    }

    @Override
    public EventBuilder<?> eventBuilder(String typeId) {
        return SimpleEvent.builder(typeId, this);
    }

    private java.util.logging.Level toJulLevel(Level level) {
        switch (level) {
            case DEBUG:
                return java.util.logging.Level.FINE;
            case INFO:
                return java.util.logging.Level.INFO;
            case WARN:
                return java.util.logging.Level.WARNING;
            case ERROR:
                return java.util.logging.Level.SEVERE;
            default:
                throw new IllegalArgumentException("Unknown Level " + level);
        }
    }

    private void logInternal(String typeId, Level level, String message, Map<String, Object> parameters, Throwable cause) {
        java.util.logging.Level julLevel = toJulLevel(level);
        if (!logger.isLoggable(julLevel)) {
            return;
        }
        StringBuilder sb = new StringBuilder(typeId).append(":");
        if (message != null) {
            sb.append(message);
        }
        sb.append(":");
        if (parameters != null && !parameters.isEmpty()) {
            sb.append(parameters);
        }
        if (cause != null) {
            logger.log(julLevel, sb.toString(), cause);
        } else {
            logger.log(julLevel, sb.toString());
        }
    }
}
