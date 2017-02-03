package com.hazelcast.client.enterprise.encryption;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.ssl.TestKeyStoreUtil;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author mdogan 8/23/13
 */

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientSSLSocketTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 60000)
    public void testClientThrowsExceptionIfNodesAreUsingSSLButClientIsNot() throws Exception {
        Properties serverSslProps = TestKeyStoreUtil.createSslProperties();
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        cfg.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(serverSslProps));
        Hazelcast.newHazelcastInstance(cfg);
        Hazelcast.newHazelcastInstance(cfg);

        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(Matchers.containsString("Unable to connect"));
        HazelcastClient.newHazelcastClient(config);
    }

    @Test
    public void test() throws IOException {
        Properties serverSslProps = TestKeyStoreUtil.createSslProperties();
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        cfg.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(serverSslProps));
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(cfg);
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(cfg);

        Properties clientSslProps = TestKeyStoreUtil.createSslProperties();
        // no need for keystore on client side
        clientSslProps.remove(TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE);
        clientSslProps.remove(TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD);
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(clientSslProps));

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<Object, Object> clientMap = client.getMap("test");

        int size = 1000;
        for (int i = 0; i < size; i++) {
            assertNull(clientMap.put(i, 2 * i + 1));
        }

        IMap<Object, Object> map = hz1.getMap("test");
        for (int i = 0; i < size; i++) {
            assertEquals(2 * i + 1, map.get(i));
        }
    }
}

