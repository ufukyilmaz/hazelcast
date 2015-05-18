package com.hazelcast.enterprise;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by ibo9 on 16/03/15.
 */
public class SampleLicense {

    public static final Properties licenses;
    public static String EXPIRED_ENTERPRISE_LICENSE, TWO_NODES_ENTERPRISE_LICENSE,
            ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART, UNLIMITED_LICENSE,
            TWO_GB_NATIVE_MEMORY_LICENSE, SECURITY_ONLY_LICENSE;

    static {
        licenses = new Properties();
        try {
            InputStream stream = SampleLicense.class.getClassLoader().getResourceAsStream("com.hazelcast.enterprise/license.properties");
            try {
                licenses.load(stream);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                EXPIRED_ENTERPRISE_LICENSE = licenses.getProperty("EXPIRED_ENTERPRISE_LICENSE");
                TWO_NODES_ENTERPRISE_LICENSE = licenses.getProperty("TWO_NODES_ENTERPRISE_LICENSE");
                ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART = licenses.getProperty("ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART");
                UNLIMITED_LICENSE = licenses.getProperty("UNLIMITED_LICENSE");
                TWO_GB_NATIVE_MEMORY_LICENSE = licenses.getProperty("TWO_GB_NATIVE_MEMORY_LICENSE");
                SECURITY_ONLY_LICENSE = licenses.getProperty("SECURITY_ONLY_LICENSE");
                stream.close();
            }
        } catch (IOException ex) {
    /* Handle exception. */
        }
    }

}
