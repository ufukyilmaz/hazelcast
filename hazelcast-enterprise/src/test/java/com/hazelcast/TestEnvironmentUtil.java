package com.hazelcast;

import io.netty.handler.ssl.OpenSsl;

import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class TestEnvironmentUtil {

    /**
     * Assumption check, that it's possible to run Hazelcast with OpenSSL on this platform and JVM.
     */
    public static void assumeThatOpenSslIsSupported() {
        assumeTrue("OpenSSL unsupported on this platform", isOpenSslSupported());
    }

    public static void assumeThatNoJDK6() {
        String javaVersion = System.getProperty("java.version");
        assumeFalse("Java 6 used", javaVersion.startsWith("1.6."));
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
