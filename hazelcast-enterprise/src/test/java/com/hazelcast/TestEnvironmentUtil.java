package com.hazelcast;

import com.hazelcast.nio.ssl.PatchedLog4j2NettyLoggerFactory;
import io.netty.handler.ssl.OpenSsl;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

import static org.junit.Assume.assumeTrue;

public class TestEnvironmentUtil {

    public static final String JAVA_VERSION = System.getProperty("java.version");
    public static final int JAVA_VERSION_MAJOR;

    static {
        String[] versionParts = JAVA_VERSION.split("\\.");
        String majorStr = versionParts[0];
        if ("1".equals(majorStr)) {
            majorStr = versionParts[1];
        }
        JAVA_VERSION_MAJOR = Integer.parseInt(majorStr);
    }

    /**
     * Assumption check, that it's possible to run Hazelcast with OpenSSL on this platform and JVM.
     */
    public static void assumeThatOpenSslIsSupported() {
        assumeTrue("OpenSSL unsupported on this platform", isOpenSslSupported());
    }

    public static void assumeThatNoJDK6() {
        assumeTrue("Test skipped for Java version 6", JAVA_VERSION_MAJOR != 6);
    }

    public static void assumeJdk8OrNewer() {
        assumeTrue("Test skipped for Java versions lower than 8", JAVA_VERSION_MAJOR >= 8);
    }

    /**
     * Returns true if it's possible to run Hazelcast with OpenSSL on this platform and JVM.
     */
    public static boolean isOpenSslSupported() {
        patchOpenSslLogging();
        return OpenSsl.isAvailable() && !isIbmJvm();
    }

    public static boolean isIbmJvm() {
        String vendor = System.getProperty("java.vendor");
        return vendor.startsWith("IBM");
    }

    private static void patchOpenSslLogging() {
        InternalLoggerFactory originFactory = InternalLoggerFactory.getDefaultFactory();
        if (!originFactory.getClass().getName().equals("io.netty.util.internal.logging.Log4J2LoggerFactory")) {
            // it's already patched or netty does not use log4j2 at all
            return;
        }

        // Netty created Log4J2LoggerFactory this implies we have log4j2 on a classpath
        // let's check what version is this
        try {
            ExtendedLoggerWrapper.class.getMethod("debug", String.class, Object.class);
            // apparently we are using a new log4j2 version! This means we live in the future
            // and no longer supporting Java 6. Hence this whole netty logging patching should
            // be removed!
            throw new AssertionError("You managed to upgrade log4j2, congratulations!"
                    + " Now let's removing the ugly netty logging patching!");
        } catch (NoSuchMethodException e) {
            // ok, this is an old version of log4j2. let's do some ugly patching

            // reasoning:
            // log4j2 2.3 is the last version supporting Java 6. However it's incompatible
            // with Netty - some methods are missing and this results in AbstractMethodError
            // so let's patch the Netty logging to use our wrapper which implements the missing
            // methods on its own.
            InternalLoggerFactory wrappedFactory = new PatchedLog4j2NettyLoggerFactory(originFactory);
            InternalLoggerFactory.setDefaultFactory(wrappedFactory);
        }
    }
}
