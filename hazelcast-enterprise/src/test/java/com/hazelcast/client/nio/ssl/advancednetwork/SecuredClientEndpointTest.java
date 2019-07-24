package com.hazelcast.client.nio.ssl.advancednetwork;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.ssl.advancednetwork.AbstractSecuredAllEndpointsTest;
import com.hazelcast.test.annotation.SlowTest;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class SecuredClientEndpointTest extends AbstractSecuredAllEndpointsTest {

    @Test
    public void testClientEndpointWithClientCertificate() {
        ClientConfig clientConfig = createClientConfig(clientTruststore, CLIENT_PASSWORD);
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

    @Test
    public void testClientEndpointWithMemberCertificate() {
        testClientEndpointWithIncorrectCertificate(memberBTruststore, MEMBER_PASSWORD);
    }

    @Test
    public void testClientEndpointWithWanCertificate() {
        testClientEndpointWithIncorrectCertificate(wanTruststore, WAN_PASSWORD);
    }

    @Test
    public void testClientEndpointWithRestCertificate() {
        testClientEndpointWithIncorrectCertificate(restTruststore, REST_PASSWORD);
    }

    @Test
    public void testClientEndpointWithMemcacheCertificate() {
        testClientEndpointWithIncorrectCertificate(memcacheTruststore, MEMCACHE_PASSWORD);
    }

    private void testClientEndpointWithIncorrectCertificate(File truststore, String password) {
        ClientConfig clientConfig = createClientConfig(truststore, password);
        HazelcastInstance client = null;
        try {
            client = HazelcastClient.newHazelcastClient(clientConfig);
            fail("Client should not be able to connect");
        } catch (IllegalStateException expected) {
            // expected
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    private ClientConfig createClientConfig(File truststore, String password) {
        SSLConfig sslConfig = new SSLConfig().setEnabled(true);
        sslConfig
                .setProperty("trustStore", truststore.getAbsolutePath())
                .setProperty("trustStorePassword", password);
        ClientConfig config = new ClientConfig();
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        List<String> addresses = new ArrayList<String>();
        addresses.add("127.0.0.1:" + CLIENT_PORT);
        networkConfig.setAddresses(addresses);
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.setConnectionTimeout(3000);
        return config;
    }
}
