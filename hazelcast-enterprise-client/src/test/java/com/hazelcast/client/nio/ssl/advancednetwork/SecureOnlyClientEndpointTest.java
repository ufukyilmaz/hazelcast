package com.hazelcast.client.nio.ssl.advancednetwork;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.ssl.advancednetwork.AbstractSecureOneEndpointTest;
import com.hazelcast.test.annotation.SlowTest;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class SecureOnlyClientEndpointTest extends AbstractSecureOneEndpointTest {

    @Test
    public void testClientConnectionToEndpoints() throws Exception {
        SSLConfig sslConfig = new SSLConfig().setEnabled(true);
        sslConfig
                .setProperty("trustStore", clientTruststore.getAbsolutePath())
                .setProperty("trustStorePassword", CLIENT_PASSWORD);
        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        List<String> addresses = new ArrayList<String>();
        addresses.add("127.0.0.1:" + CLIENT_PORT);
        networkConfig.setAddresses(addresses);
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.setConnectionTimeout(3000);
        HazelcastInstance client = null;
        try {
            client = HazelcastClient.newHazelcastClient(clientConfig);
            client.getMap("testClientMap").put("a", "b");
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @Override
    protected Properties prepareClientEndpointSsl() {
        return prepareSslProperties(memberForClientKeystore, CLIENT_PASSWORD);
    }
}
