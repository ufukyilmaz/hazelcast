package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE_PASSWORD;
import static com.hazelcast.test.HazelcastTestSupport.assertEqualsEventually;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class SecureOnlyWanEndpointTest extends AbstractSecureOneEndpointTest {

    @Test
    @Override
    public void testWanConnectionToEndpoints() {
        Properties props = new Properties();
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE, wanTruststore.getAbsolutePath());
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, WAN_PASSWORD);
        Config config2 = prepareWanAdvancedNetworkConfig(WAN_PORT, props);
        HazelcastInstance hz2 = null;
        try {
            hz2 = Hazelcast.newHazelcastInstance(config2);
            IMap<String, String> map = hz2.getMap(REPLICATED_MAP);
            map.put("someKey", "someValue");

            assertEqualsEventually(new Callable<String>() {
                @Override
                public String call() {
                    IMap<String, String> map1 = hz.getMap(REPLICATED_MAP);
                    return map1.get("someKey");
                }
            }, "someValue");
        } finally {
            if (hz2 != null) {
                hz2.shutdown();
            }
        }
    }

    @Override
    protected Properties prepareWanEndpointSsl() {
        return prepareSslProperties(memberForWanKeystore, WAN_PASSWORD);
    }
}
