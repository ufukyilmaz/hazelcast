package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.hotrestart.impl.di.Inject;

import java.util.logging.Level;

/**
 * Adds lazy-evaluating methods to Hazelcast logger.
 */
public class GcLogger {
    private final ILogger logger;

    @Inject
    GcLogger(ILogger logger) {
        this.logger = logger;
    }

    public void finest(String template, Object arg) {
        if (isFinestEnabled()) {
            finest(String.format(template, arg));
        }
    }

    public void fine(String template, Object arg) {
        if (logger.isFineEnabled()) {
            fine(String.format(template, arg));
        }
    }

    public void fine(String template, Object arg1, Object arg2) {
        if (logger.isFineEnabled()) {
            fine(String.format(template, arg1, arg2));
        }
    }

    public void fine(String template, Object arg1, Object arg2, Object arg3) {
        if (logger.isFineEnabled()) {
            fine(String.format(template, arg1, arg2, arg3));
        }
    }

    public void fine(String template, Object arg1, Object arg2, Object arg3, Object arg4) {
        if (logger.isFineEnabled()) {
            fine(String.format(template, arg1, arg2, arg3, arg4));
        }
    }

    public void fine(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        if (logger.isFineEnabled()) {
            fine(String.format(template, arg1, arg2, arg3, arg4, arg5));
        }
    }

    public void fine(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        if (logger.isFineEnabled()) {
            fine(String.format(template, arg1, arg2, arg3, arg4, arg5, arg6));
        }
    }

    public void info(String template, Object arg1) {
        if (logger.isLoggable(Level.INFO)) {
            info(String.format(template, arg1));
        }
    }

    public void info(String template, Object arg1, Object arg2) {
        if (logger.isLoggable(Level.INFO)) {
            info(String.format(template, arg1, arg2));
        }
    }

    public void info(String template, Object arg1, Object arg2, Object arg3) {
        if (logger.isLoggable(Level.INFO)) {
            info(String.format(template, arg1, arg2, arg3));
        }
    }

    public void info(String template, Object arg1, Object arg2, Object arg3, Object arg4) {
        if (logger.isLoggable(Level.INFO)) {
            info(String.format(template, arg1, arg2, arg3, arg4));
        }
    }

    public void info(String template,
                     Object arg1, Object arg2, Object arg3, Object arg4, Object arg5
    ) {
        if (logger.isLoggable(Level.INFO)) {
            info(String.format(template, arg1, arg2, arg3, arg4, arg5));
        }
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public void info(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6,
                     Object arg7, Object arg8) {
        if (logger.isLoggable(Level.INFO)) {
            info(String.format(template, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8));
        }
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public void info(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6,
                     Object arg7, Object arg8, Object arg9) {
        if (logger.isLoggable(Level.INFO)) {
            info(String.format(template, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9));
        }
    }

    boolean isFineEnabled() {
        return logger.isFineEnabled();
    }
    boolean isFinestEnabled() {
        return logger.isFinestEnabled();
    }
    public void finest(String message) {
        logger.finest(message);
    }
    public void fine(String message) {
        logger.fine(message);
    }
    public void info(String message) {
        logger.info(message);
    }
    public void severe(String message, Throwable thrown) {
        logger.severe(message, thrown);
    }
    public void warning(String message, Throwable thrown) {
        logger.warning(message, thrown);
    }
    public void severe(String message) {
        logger.severe(message);
    }
    public void warning(String message) {
        logger.warning(message);
    }
}
