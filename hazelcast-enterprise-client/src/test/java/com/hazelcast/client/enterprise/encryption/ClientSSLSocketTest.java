package com.hazelcast.client.enterprise.encryption;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.ssl.TestKeyStoreUtil;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_MUTUAL_AUTHENTICATION;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getWrongKeyStoreFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 8/23/13
 */

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientSSLSocketTest {

    @After
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 60000, expected = IllegalStateException.class)
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
        HazelcastClient.newHazelcastClient(config);
    }

    @Test
    public void test() throws IOException {
        Properties serverSslProps = TestKeyStoreUtil.createSslProperties();
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        cfg.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(serverSslProps));
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(cfg);
        Hazelcast.newHazelcastInstance(cfg);

        Properties clientSslProps = TestKeyStoreUtil.createSslProperties();
        // no need for keystore on client side
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE);
        clientSslProps.remove(TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD);
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(clientSslProps));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
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

    @Test
    public void testServerRequiresClientAuth_clientHaveKeystore() throws IOException {
        Properties serverSslProps = TestKeyStoreUtil.createSslProperties();
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        SSLConfig sslConfig = new SSLConfig().setEnabled(true).setProperties(serverSslProps);

        sslConfig.setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION,"REQUIRED");

        cfg.getNetworkConfig().setSSLConfig(sslConfig);
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(cfg);
        Hazelcast.newHazelcastInstance(cfg);

        Properties clientSslProps = TestKeyStoreUtil.createSslProperties();
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(clientSslProps));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test(expected = IllegalStateException.class)
    public void testServerRequiresClientAuth_clientDontHaveKeystore() throws IOException {
        Properties serverSslProps = TestKeyStoreUtil.createSslProperties();
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        SSLConfig sslConfig = new SSLConfig().setEnabled(true).setProperties(serverSslProps);

        sslConfig.setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION,"REQUIRED");

        cfg.getNetworkConfig().setSSLConfig(sslConfig);

        Hazelcast.newHazelcastInstance(cfg);
        Hazelcast.newHazelcastInstance(cfg);

        Properties clientSslProps = TestKeyStoreUtil.createSslProperties();
        // no need for keystore on client side
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE);
        clientSslProps.remove(TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD);
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(clientSslProps));

        HazelcastClient.newHazelcastClient(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testServerRequiresClientAuth_clientHaveWrongKeystore() throws IOException {
        Properties serverSslProps = TestKeyStoreUtil.createSslProperties();
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        SSLConfig sslConfig = new SSLConfig().setEnabled(true).setProperties(serverSslProps);

        sslConfig.setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION,"REQUIRED");

        cfg.getNetworkConfig().setSSLConfig(sslConfig);
        Hazelcast.newHazelcastInstance(cfg);
        Hazelcast.newHazelcastInstance(cfg);

        Properties clientSslProps = TestKeyStoreUtil.createSslProperties();
        clientSslProps.setProperty(JAVAX_NET_SSL_KEY_STORE, getWrongKeyStoreFilePath());
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(clientSslProps));

        HazelcastClient.newHazelcastClient(config);
    }

    @Test
    public void testOptionalClientAuth_clientHaveKeystore() throws IOException {
        Properties serverSslProps = TestKeyStoreUtil.createSslProperties();
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        SSLConfig sslConfig = new SSLConfig().setEnabled(true).setProperties(serverSslProps);

        sslConfig.setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION,"OPTIONAL");

        cfg.getNetworkConfig().setSSLConfig(sslConfig);
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(cfg);
        Hazelcast.newHazelcastInstance(cfg);

        Properties clientSslProps = TestKeyStoreUtil.createSslProperties();
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(clientSslProps));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test
    public void testOptionalClientAuth_clientHaveWrongKeystore() throws IOException {
        Properties serverSslProps = TestKeyStoreUtil.createSslProperties();
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        SSLConfig sslConfig = new SSLConfig().setEnabled(true).setProperties(serverSslProps);

        sslConfig.setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION,"OPTIONAL");

        cfg.getNetworkConfig().setSSLConfig(sslConfig);
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(cfg);
        Hazelcast.newHazelcastInstance(cfg);

        Properties clientSslProps = TestKeyStoreUtil.createSslProperties();
        clientSslProps.setProperty(JAVAX_NET_SSL_KEY_STORE, getWrongKeyStoreFilePath());
        clientSslProps.setProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, "123456");
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(clientSslProps));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test
    public void testOptionalClientAuth_clientDontHaveKeystore() throws IOException {
        Properties serverSslProps = TestKeyStoreUtil.createSslProperties();
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        SSLConfig sslConfig = new SSLConfig().setEnabled(true).setProperties(serverSslProps);

        sslConfig.setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION,"OPTIONAL");

        cfg.getNetworkConfig().setSSLConfig(sslConfig);
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(cfg);
        Hazelcast.newHazelcastInstance(cfg);

        Properties clientSslProps = TestKeyStoreUtil.createSslProperties();
        // remove keystore on client properties side
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE);
        clientSslProps.remove(TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD);
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(clientSslProps));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test
    public void testMalformedKeystore_onClient() throws IOException {
        Properties serverSslProps = TestKeyStoreUtil.createSslProperties();
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        SSLConfig sslConfig = new SSLConfig().setEnabled(true).setProperties(serverSslProps);

        sslConfig.setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION,"OPTIONAL");

        cfg.getNetworkConfig().setSSLConfig(sslConfig);
        Hazelcast.newHazelcastInstance(cfg);

        Properties clientSslProps = TestKeyStoreUtil.createSslProperties();
        // remove keystore on client properties side
        clientSslProps.put(TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE, TestKeyStoreUtil.getMalformedKeyStoreFilePath());
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(clientSslProps));

        try {
            HazelcastClient.newHazelcastClient(config);
        } catch (HazelcastException e){
            assertEquals(e.getCause().getClass(), IOException.class);
            assertTrue(e.getCause().getMessage().contains("Invalid keystore format"));
        }
    }
}

