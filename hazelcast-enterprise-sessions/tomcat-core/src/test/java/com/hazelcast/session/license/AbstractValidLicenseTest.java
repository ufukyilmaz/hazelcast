package com.hazelcast.session.license;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.session.AbstractHazelcastSessionsTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public abstract class AbstractValidLicenseTest extends AbstractHazelcastSessionsTest {

    @After
    @Override
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
        instance1.stop();
    }

    @Test
    public void testClientServerWithValidLicense() throws Exception{
        Hazelcast.newHazelcastInstance();
        instance1 = getWebContainerConfigurator();
        instance1.port(SERVER_PORT_1).sticky(false).clientOnly(true).sessionTimeout(10).start();
    }

    @Test
    public void testP2PWithValidLicense() throws Exception{
        instance1 = getWebContainerConfigurator();
        instance1.port(SERVER_PORT_1).sticky(false).clientOnly(false).sessionTimeout(10).start();
    }
}
