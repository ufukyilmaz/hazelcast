package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.hotrestart.impl.di.Inject;

import java.util.logging.Level;

/**
 * Adds lazy-evaluating methods to Hazelcast logger.
 */
public class GcLogger {
    public static final String VERBOSE_FINEST_LOGGING = "hazelcast.hotrestart.gc.verboseFinestLogging";

    private final ILogger logger;
    private final boolean finestVerboseEnabled = Boolean.getBoolean(VERBOSE_FINEST_LOGGING);

    @Inject
    GcLogger(ILogger logger) {
        this.logger = logger;
    }

    public void finestVerbose(String message) {
        if (finestVerboseEnabled) {
            logger.finest(message);
        }
    }

    public void finestVerbose(String template, Object arg) {
        if (finestVerboseEnabled && logger.isFinestEnabled()) {
            logger.finest(String.format(template, arg));
        }
    }

    public void finest(String message) {
        logger.fine(message);
    }

    public void finest(String template, Object arg) {
        if (logger.isFineEnabled()) {
            finest(String.format(template, arg));
        }
    }

    public void finest(String template, Object arg1, Object arg2) {
        if (logger.isFineEnabled()) {
            finest(String.format(template, arg1, arg2));
        }
    }

    public void finest(String template, Object arg1, Object arg2, Object arg3) {
        if (logger.isFineEnabled()) {
            finest(String.format(template, arg1, arg2, arg3));
        }
    }

    public void finest(String template, Object arg1, Object arg2, Object arg3, Object arg4) {
        if (logger.isFineEnabled()) {
            finest(String.format(template, arg1, arg2, arg3, arg4));
        }
    }

    public void finest(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        if (logger.isFineEnabled()) {
            finest(String.format(template, arg1, arg2, arg3, arg4, arg5));
        }
    }

    public void finest(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        if (logger.isFineEnabled()) {
            finest(String.format(template, arg1, arg2, arg3, arg4, arg5, arg6));
        }
    }

    public void fine(String message) {
        logger.info(message);
    }

    public void fine(String template, Object arg1) {
        if (logger.isLoggable(Level.INFO)) {
            fine(String.format(template, arg1));
        }
    }

    public void fine(String template, Object arg1, Object arg2) {
        if (logger.isLoggable(Level.INFO)) {
            fine(String.format(template, arg1, arg2));
        }
    }

    public void fine(String template, Object arg1, Object arg2, Object arg3) {
        if (logger.isLoggable(Level.INFO)) {
            fine(String.format(template, arg1, arg2, arg3));
        }
    }

    public void fine(String template, Object arg1, Object arg2, Object arg3, Object arg4) {
        if (logger.isLoggable(Level.INFO)) {
            fine(String.format(template, arg1, arg2, arg3, arg4));
        }
    }

    public void fine(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        if (logger.isLoggable(Level.INFO)) {
            fine(String.format(template, arg1, arg2, arg3, arg4, arg5));
        }
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public void fine(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6,
                     Object arg7, Object arg8) {
        if (logger.isLoggable(Level.INFO)) {
            fine(String.format(template, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8));
        }
    }

    public void warning(String message) {
        logger.warning(message);
    }

    public void severe(String message) {
        logger.severe(message);
    }

    public void severe(String message, Throwable thrown) {
        logger.severe(message, thrown);
    }

    boolean isFinestEnabled() {
        return logger.isFinestEnabled();
    }
}
