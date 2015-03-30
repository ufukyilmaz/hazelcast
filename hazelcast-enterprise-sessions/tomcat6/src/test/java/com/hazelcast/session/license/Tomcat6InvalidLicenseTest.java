package com.hazelcast.session.license;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.session.AbstractHazelcastSessionsTest;
import com.hazelcast.session.Tomcat6Configurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.junit.After;
import org.junit.Test;

public class Tomcat6InvalidLicenseTest extends AbstractHazelcastSessionsTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat6Configurator("hazelcast-without-license.xml","hazelcast-client-without-license.xml");
    }

    @After
    @Override
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test(expected = InvalidLicenseException.class)
    public void testClientServerWithInvalidLicense() throws Exception{
        Hazelcast.newHazelcastInstance();
        instance1 = getWebContainerConfigurator();
        instance1.port(SERVER_PORT_1).sticky(false).clientOnly(true).sessionTimeout(10).start();
    }

    @Test(expected = InvalidLicenseException.class)
    public void testP2PWithInvalidLicense() throws Exception{
        instance1 = getWebContainerConfigurator();
        instance1.port(SERVER_PORT_1).sticky(false).clientOnly(false).sessionTimeout(10).start();
    }

}
