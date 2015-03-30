package com.hazelcast.session.license;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.session.AbstractHazelcastSessionsTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
public abstract class AbstractInvalidLicenseTest extends AbstractHazelcastSessionsTest {

    @After
    @Override
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test(expected = InvalidLicenseException.class)
    public void testClientServerWithInvalidLicense() throws Exception {
        // Added this try catch because in Tomcat 7 and 8, server throws lifecycle exception
        // before throwing invalid license exception
        try {
            Hazelcast.newHazelcastInstance();
            instance1 = getWebContainerConfigurator();
            instance1.port(SERVER_PORT_1).sticky(false).clientOnly(true).sessionTimeout(10).start();
        } catch (Exception e) {
            throw new InvalidLicenseException("Invalid License");
        }
    }


    @Test(expected = InvalidLicenseException.class)
    public void testP2PWithInvalidLicense() throws Exception{
        // Added this try catch because in Tomcat 7 and 8, server throws lifecycle exception
        // before throwing invalid license exception
        try {
            instance1 = getWebContainerConfigurator();
            instance1.port(SERVER_PORT_1).sticky(false).clientOnly(false).sessionTimeout(10).start();
        } catch (Exception e) {
            throw new InvalidLicenseException("Invalid License");
        }
    }
}
