package com.hazelcast;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ssl.PatchedLog4j2NettyLoggerFactory;
import io.netty.handler.ssl.OpenSsl;
import io.netty.util.internal.logging.InternalLoggerFactory;

import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.nio.IOUtil.copy;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class TestEnvironmentUtil {

    public static final String JAVA_VERSION = System.getProperty("java.version");
    public static final int JAVA_VERSION_MAJOR;

    private static final ILogger LOGGER = Logger.getLogger(TestEnvironmentUtil.class);

    static {
        String[] versionParts = JAVA_VERSION.split("\\.");
        String majorStr = versionParts[0];
        if ("1".equals(majorStr)) {
            majorStr = versionParts[1];
        }
        // early access builds could contain "-ea" suffix, we have to remove it
        majorStr = majorStr.split("-")[0];
        JAVA_VERSION_MAJOR = Integer.parseInt(majorStr);
    }

    /**
     * Assumption check, that it's possible to run Hazelcast with OpenSSL on this platform and JVM.
     */
    public static void assumeThatOpenSslIsSupported() {
        assumeTrue("OpenSSL unsupported on this platform", isOpenSslSupported());
    }

    public static void assumeThatNoJDK6() {
        assumeJavaVersionAtLeast(7);
    }

    public static void assumeJdk8OrNewer() {
        assumeJavaVersionAtLeast(8);
    }

    public static void assumeJavaVersionAtLeast(int minimalVersion) {
        assumeTrue("Test skipped for Java versions lower than " + minimalVersion, JAVA_VERSION_MAJOR >= minimalVersion);
    }

    public static void assumeNoIbmJvm() {
        assumeFalse("Test skipped for IBM Java", isIbmJvm());
    }

    /**
     * Returns true if it's possible to run Hazelcast with OpenSSL on this platform and JVM.
     */
    public static boolean isOpenSslSupported() {
        patchOpenSslLogging();
        return Boolean.getBoolean("openssl.enforce") || OpenSsl.isAvailable();
    }

    public static boolean isIbmJvm() {
        String vendor = System.getProperty("java.vendor");
        return vendor.startsWith("IBM");
    }

    /**
     * Copies a resource file from given class package to target folder. The target is
     * denoted by given {@link java.io.File} instance.
     */
    public static File copyTestResource(Class<?> testClass, File targetFolder, String resourceName) {
        File targetFile = new File(targetFolder, resourceName);
        if (!targetFile.exists()) {
            try {
                assertTrue(targetFile.createNewFile());
                LOGGER.info("Copying test resource to file " + targetFile.getAbsolutePath());
                InputStream is = null;
                try {
                    is = testClass.getResourceAsStream(resourceName);
                    copy(is, targetFile);
                } finally {
                    closeResource(is);
                }
            } catch (IOException e) {
                fail("Unable to copy test resource " + resourceName + " to " + targetFolder + ": " + e.getMessage());
            }
        }
        return targetFile;
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
