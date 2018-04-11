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
import com.hazelcast.nio.ssl.OpenSSLEngineFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.TestEnvironmentUtil.assumeThatOpenSslIsSupported;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_MUTUAL_AUTHENTICATION;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.createSslProperties;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getMalformedKeyStoreFilePath;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getWrongKeyStoreFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientSSLSocketTest {

    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 60000, expected = IllegalStateException.class)
    public void testClientThrowsExceptionIfNodesAreUsingSSLButClientIsNot() throws Exception {
        Config serverConfig = new Config();
        serverConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        serverConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        serverConfig.getNetworkConfig().setSSLConfig(getSslConfig());
        Hazelcast.newHazelcastInstance(serverConfig);
        Hazelcast.newHazelcastInstance(serverConfig);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1");
        HazelcastClient.newHazelcastClient(clientConfig);
    }


    @Test
    public void testClientOpenSSL_serverOpenSSL() throws Exception {
        assumeThatOpenSslIsSupported();
        test(true, true);
    }

    @Test
    public void testClientOpenSSL_serverSSLEngine() throws Exception {
        assumeThatOpenSslIsSupported();
        test(true, false);
    }

    @Test
    public void testClientSSLEngine_serverOpenSSL() throws Exception {
        assumeThatOpenSslIsSupported();
        test(false, true);
    }

    @Test
    public void testClientSSLEngine_serverSSLEngine() throws Exception {
        test(false, false);
    }

    public void test(boolean serverOpenSSL, boolean clientOpenSSL) throws Exception {
        Config serverConfig = new Config();
        serverConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        serverConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        SSLConfig serverSSLConfig = getSslConfig();
        if (serverOpenSSL) {
            serverSSLConfig.setFactoryClassName(OpenSSLEngineFactory.class.getName());
        }
        serverConfig.getNetworkConfig().setSSLConfig(serverSSLConfig);

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(serverConfig);
        Hazelcast.newHazelcastInstance(serverConfig);

        // no need for keystore on client side
        Properties clientSslProps = createSslProperties();
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE);
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE_PASSWORD);
        ClientConfig clientConfig = new ClientConfig();
        SSLConfig clientSSLConfig = getSslConfig(clientSslProps);
        if (clientOpenSSL) {
            clientSSLConfig.setFactoryClassName(OpenSSLEngineFactory.class.getName());
        }
        clientConfig.getNetworkConfig().addAddress("127.0.0.1")
                .setRedoOperation(true)
                .setSSLConfig(clientSSLConfig);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
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
    public void testServerRequiresClientAuth_clientHaveKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig()
                .setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "REQUIRED");
        Config serverConfig = new Config();
        serverConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        serverConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        serverConfig.getNetworkConfig().setSSLConfig(sslConfig);

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(serverConfig);
        Hazelcast.newHazelcastInstance(serverConfig);

        Properties clientSslProps = createSslProperties();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig()
                .addAddress("127.0.0.1")
                .setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test(expected = IllegalStateException.class)
    public void testServerRequiresClientAuth_clientDoesNotHaveKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig()
                .setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "REQUIRED");
        Config serverConfig = new Config();
        serverConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        serverConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        serverConfig.getNetworkConfig().setSSLConfig(sslConfig);

        Hazelcast.newHazelcastInstance(serverConfig);
        Hazelcast.newHazelcastInstance(serverConfig);

        // no need for keystore on client side
        Properties clientSslProps = createSslProperties();
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE);
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE_PASSWORD);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig()
                .addAddress("127.0.0.1")
                .setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Test(expected = IllegalStateException.class)
    public void testServerRequiresClientAuth_clientHaveWrongKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig()
                .setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "REQUIRED");
        Config serverConfig = new Config();
        serverConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        serverConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        serverConfig.getNetworkConfig().setSSLConfig(sslConfig);

        Hazelcast.newHazelcastInstance(serverConfig);
        Hazelcast.newHazelcastInstance(serverConfig);

        Properties clientSslProps = createSslProperties();
        clientSslProps.setProperty(JAVAX_NET_SSL_KEY_STORE, getWrongKeyStoreFilePath());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1")
                .setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Test
    public void testOptionalClientAuth_clientHaveKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig()
                .setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "OPTIONAL");
        Config serverConfig = new Config();
        serverConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        serverConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        serverConfig.getNetworkConfig().setSSLConfig(sslConfig);

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(serverConfig);
        Hazelcast.newHazelcastInstance(serverConfig);

        Properties clientSslProps = createSslProperties();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig()
                .addAddress("127.0.0.1")
                .setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test
    public void testOptionalClientAuth_clientHaveWrongKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig()
                .setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "OPTIONAL");
        Config serverConfig = new Config();
        serverConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        serverConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        serverConfig.getNetworkConfig().setSSLConfig(sslConfig);

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(serverConfig);
        Hazelcast.newHazelcastInstance(serverConfig);

        Properties clientSslProps = createSslProperties();
        clientSslProps.setProperty(JAVAX_NET_SSL_KEY_STORE, getWrongKeyStoreFilePath());
        clientSslProps.setProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, "123456");
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig()
                .addAddress("127.0.0.1")
                .setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test
    public void testOptionalClientAuth_clientDoesNotHaveKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig()
                .setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "OPTIONAL");
        Config serverConfig = new Config();
        serverConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        serverConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        serverConfig.getNetworkConfig().setSSLConfig(sslConfig);

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(serverConfig);
        Hazelcast.newHazelcastInstance(serverConfig);

        // remove keystore on client properties side
        Properties clientSslProps = createSslProperties();
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE);
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE_PASSWORD);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig()
                .addAddress("127.0.0.1")
                .setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test
    public void testMalformedKeystore_onClient() throws Exception {
        SSLConfig sslConfig = getSslConfig()
                .setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "OPTIONAL");
        Config serverConfig = new Config();
        serverConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        serverConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        serverConfig.getNetworkConfig().setSSLConfig(sslConfig);

        Hazelcast.newHazelcastInstance(serverConfig);

        // remove keystore on client properties side
        Properties clientSslProps = createSslProperties();
        clientSslProps.put(JAVAX_NET_SSL_KEY_STORE, getMalformedKeyStoreFilePath());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig()
                .addAddress("127.0.0.1")
                .setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        try {
            HazelcastClient.newHazelcastClient(clientConfig);
        } catch (HazelcastException e) {
            assertEquals(e.getCause().getClass(), IOException.class);
            assertTrue(e.getCause().getMessage().contains("Invalid keystore format"));
        }
    }

    private static SSLConfig getSslConfig() throws Exception {
        return getSslConfig(createSslProperties());
    }

    private static SSLConfig getSslConfig(Properties sslProps) throws Exception {
        return new SSLConfig().setEnabled(true).setProperties(sslProps);
    }
}
