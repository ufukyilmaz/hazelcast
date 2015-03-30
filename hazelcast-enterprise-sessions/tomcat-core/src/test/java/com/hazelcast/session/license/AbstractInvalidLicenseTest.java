package com.hazelcast.session.license;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.session.AbstractHazelcastSessionsTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
public abstract class AbstractInvalidLicenseTest extends AbstractHazelcastSessionsTest {

    Exception exceptionToBeThrown;

    @After
    @Override
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testClientServerWithInvalidLicense() throws Exception {
        expectedEx.expect(exceptionToBeThrown.getClass());
        Hazelcast.newHazelcastInstance();
        instance1 = getWebContainerConfigurator();
        instance1.port(SERVER_PORT_1).sticky(false).clientOnly(true).sessionTimeout(10).start();
    }

    @Test
    public void testP2PWithInvalidLicense() throws Exception {
        expectedEx.expect(exceptionToBeThrown.getClass());
        instance1 = getWebContainerConfigurator();
        instance1.port(SERVER_PORT_1).sticky(false).clientOnly(false).sessionTimeout(10).start();
    }
}
