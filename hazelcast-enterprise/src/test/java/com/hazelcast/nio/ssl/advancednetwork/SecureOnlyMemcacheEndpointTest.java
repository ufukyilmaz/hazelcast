package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import java.util.Properties;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class SecureOnlyMemcacheEndpointTest extends AbstractSecureOneEndpointTest {

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast-enterprise/issues/2783")
    @Override
    public void testMemcacheConnectionToEndpoints() throws Exception {
        testTextEndpoint(MEMCACHE_PORT, memcacheTruststore, MEMCACHE_PASSWORD, true);
    }

    @Override
    protected Properties prepareMemcacheEndpointSsl() {
        return prepareSslProperties(memberForMemcacheKeystore, MEMCACHE_PASSWORD);
    }
}
