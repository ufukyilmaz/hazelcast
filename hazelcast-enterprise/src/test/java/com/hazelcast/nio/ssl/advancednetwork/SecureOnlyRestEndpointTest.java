package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import java.util.Properties;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class SecureOnlyRestEndpointTest extends AbstractSecureOneEndpointTest {

    @Test
    @Override
    public void testRestConnectionToEndpoints() throws Exception {
        testTextEndpoint(REST_PORT, restTruststore, REST_PASSWORD, true);
    }

    @Override
    protected Properties prepareRestEndpointSsl() {
        return prepareSslProperties(memberForRestKeystore, REST_PASSWORD);
    }
}
