package com.hazelcast;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import io.netty.handler.ssl.OpenSsl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.nio.IOUtil.copy;
import static com.hazelcast.nio.IOUtil.toByteArray;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

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

    public static void assumeJavaVersionLessThan(int maxVersion) {
        assumeTrue("Test skipped for Java versions greater or equal to " + maxVersion, JAVA_VERSION_MAJOR < maxVersion);
    }

    public static void assumeNoIbmJvm() {
        assumeFalse("Test skipped for IBM Java", isIbmJvm());
    }

    /**
     * Returns true if it's possible to run Hazelcast with OpenSSL on this platform and JVM.
     */
    public static boolean isOpenSslSupported() {
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
        if (! targetFile.getParentFile().isDirectory()) {
            assertTrue("Creating a target folder for a test resource failed - " + resourceName,
                    targetFile.getParentFile().mkdirs());
        }
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

    /**
     * Reads resource by using {@link Class#getResourceAsStream(String)} for given class.
     * @param clazz class used to load the resource
     * @param resourcePath path to the resource
     * @return resource content as byte array
     */
    public static byte[] readResource(Class<?> clazz, String resourcePath) {
        try (InputStream is = clazz.getResourceAsStream(resourcePath)) {
            return toByteArray(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
