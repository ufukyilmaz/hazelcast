package com.hazelcast.session.license;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.session.AbstractHazelcastSessionsTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
public abstract class AbstractInvalidLicenseTest extends AbstractHazelcastSessionsTest {

    @After
    @Override
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testClientServerWithInvalidLicense() throws Exception {
        expectedEx.expect(getException());
        expectedEx.expectCause(getCause());
        instance1 = getWebContainerConfigurator();
        instance1.port(SERVER_PORT_1).sticky(false).clientOnly(true).sessionTimeout(10).start();
    }

    @Test
    public void testP2PWithInvalidLicense() throws Exception {
        expectedEx.expect(getException());
        expectedEx.expectCause(getCause());
        instance1 = getWebContainerConfigurator();
        instance1.port(SERVER_PORT_1).sticky(false).clientOnly(false).sessionTimeout(10).start();
    }

    protected abstract Matcher<? extends Throwable> getCause();

    protected abstract Class<? extends Throwable> getException();

}
