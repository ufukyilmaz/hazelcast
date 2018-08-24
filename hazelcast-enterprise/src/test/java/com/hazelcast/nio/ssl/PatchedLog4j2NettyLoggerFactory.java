package com.hazelcast.nio.ssl;

import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;

/**
 * Patched log4j2 netty logger factory.
 *
 * Reasoning: Netty requires Java 6, but its Log4j2 logger implementation
 * requires a version of Log4j2 which does not work on Java 6.
 *
 * Netty with old Log4j2 version throws AbstractMethodError. This factory produces
 * loggers which bypass the missing methods.
 *
 */
public final class PatchedLog4j2NettyLoggerFactory extends InternalLoggerFactory {
    private final Log4J2LoggerFactory delegate;

    public PatchedLog4j2NettyLoggerFactory(InternalLoggerFactory delegate) {
        this.delegate = (Log4J2LoggerFactory) delegate;
    }

    @Override
    protected InternalLogger newInstance(String s) {
        return new DelegatingLogger(delegate.newInstance(s));
    }

    private static class DelegatingLogger implements InternalLogger {
        private final InternalLogger delegate;

        private DelegatingLogger(InternalLogger delegate) {
            this.delegate = delegate;
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public boolean isTraceEnabled() {
            return delegate.isTraceEnabled();
        }

        @Override
        public void trace(String s) {
            delegate.trace(s);
        }

        @Override
        public void trace(String s, Object o) {
            delegate.trace(s, new Object[]{o});
        }

        @Override
        public void trace(String s, Object o, Object o1) {
            delegate.trace(s, new Object[]{o, o1});
        }

        @Override
        public void trace(String s, Object... objects) {
            delegate.trace(s, objects);
        }

        @Override
        public void trace(String s, Throwable throwable) {
            delegate.trace(s, throwable);
        }

        @Override
        public void trace(Throwable throwable) {
            delegate.trace(throwable);
        }

        @Override
        public boolean isDebugEnabled() {
            return delegate.isDebugEnabled();
        }

        @Override
        public void debug(String s) {
            delegate.debug(s);
        }

        @Override
        public void debug(String s, Object o) {
            delegate.debug(s, new Object[]{o});
        }

        @Override
        public void debug(String s, Object o, Object o1) {
            delegate.debug(s, new Object[]{o, o1});
        }

        @Override
        public void debug(String s, Object... objects) {
            delegate.debug(s, objects);
        }

        @Override
        public void debug(String s, Throwable throwable) {
            delegate.debug(s, throwable);
        }

        @Override
        public void debug(Throwable throwable) {
            delegate.debug(throwable);
        }

        @Override
        public boolean isInfoEnabled() {
            return delegate.isInfoEnabled();
        }

        @Override
        public void info(String s) {
            delegate.info(s);
        }

        @Override
        public void info(String s, Object o) {
            delegate.info(s, new Object[]{o});
        }

        @Override
        public void info(String s, Object o, Object o1) {
            delegate.info(s, new Object[]{o, o1});
        }

        @Override
        public void info(String s, Object... objects) {
            delegate.info(s, objects);
        }

        @Override
        public void info(String s, Throwable throwable) {
            delegate.info(s, throwable);
        }

        @Override
        public void info(Throwable throwable) {
            delegate.info(throwable);
        }

        @Override
        public boolean isWarnEnabled() {
            return delegate.isWarnEnabled();
        }

        @Override
        public void warn(String s) {
            delegate.warn(s);
        }

        @Override
        public void warn(String s, Object o) {
            delegate.warn(s, new Object[]{o});
        }

        @Override
        public void warn(String s, Object... objects) {
            delegate.warn(s, objects);
        }

        @Override
        public void warn(String s, Object o, Object o1) {
            delegate.warn(s, new Object[]{o, o1});
        }

        @Override
        public void warn(String s, Throwable throwable) {
            delegate.warn(s, throwable);
        }

        @Override
        public void warn(Throwable throwable) {
            delegate.warn(throwable);
        }

        @Override
        public boolean isErrorEnabled() {
            return delegate.isErrorEnabled();
        }

        @Override
        public void error(String s) {
            delegate.error(s);
        }

        @Override
        public void error(String s, Object o) {
            delegate.error(s, new Object[]{o});
        }

        @Override
        public void error(String s, Object o, Object o1) {
            delegate.error(s, new Object[]{o, o1});
        }

        @Override
        public void error(String s, Object... objects) {
            delegate.error(s, objects);
        }

        @Override
        public void error(String s, Throwable throwable) {
            delegate.error(s, throwable);
        }

        @Override
        public void error(Throwable throwable) {
            delegate.error(throwable);
        }

        @Override
        public boolean isEnabled(InternalLogLevel internalLogLevel) {
            return delegate.isEnabled(internalLogLevel);
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s) {
            delegate.log(internalLogLevel, s);
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object o) {
            delegate.log(internalLogLevel, s, o);
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object o, Object o1) {
            delegate.log(internalLogLevel, s, o, o1);
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object... objects) {
            delegate.log(internalLogLevel, s, objects);
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Throwable throwable) {
            delegate.log(internalLogLevel, s, throwable);
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, Throwable throwable) {
            delegate.log(internalLogLevel, throwable);
        }
    }
}
