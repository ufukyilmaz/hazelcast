package com.hazelcast;

public class IbmUtil {
    public static boolean ibmJvm() {
        String vendor = System.getProperty("java.vendor");
        return vendor.startsWith("IBM");
    }
}
