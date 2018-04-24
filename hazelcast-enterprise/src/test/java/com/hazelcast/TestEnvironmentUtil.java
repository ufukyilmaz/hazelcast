package com.hazelcast;

import io.netty.handler.ssl.OpenSsl;

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
        return OpenSsl.isAvailable() && !isIbmJvm();
    }

    public static boolean isIbmJvm() {
        String vendor = System.getProperty("java.vendor");
        return vendor.startsWith("IBM");
    }
}
