package com.hazelcast;

import io.netty.handler.ssl.OpenSsl;

import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class TestEnvironmentUtil {

    public static void assumeThatOpenSslIsAvailable() {
        assumeTrue(OpenSsl.isAvailable());
    }

    public static void assumeThatNoIbmJvm() {
        String vendor = System.getProperty("java.vendor");
        assumeFalse(vendor.startsWith("IBM"));
    }

    public static void assumeThatNoJDK6() {
        String javaVersion = System.getProperty("java.version");
        assumeFalse(javaVersion.startsWith("1.6."));
    }
}
