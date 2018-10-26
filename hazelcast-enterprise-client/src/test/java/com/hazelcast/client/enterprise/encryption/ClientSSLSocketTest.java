package com.hazelcast.client.enterprise.encryption;

import static com.hazelcast.TestEnvironmentUtil.assumeJdk8OrNewer;
import static com.hazelcast.TestEnvironmentUtil.assumeThatOpenSslIsSupported;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_FILE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_MUTUAL_AUTHENTICATION;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.createSslProperties;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getMalformedKeyStoreFilePath;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getWrongKeyStoreFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.TestEnvironmentUtil;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.nio.ssl.OpenSSLEngineFactory;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientSSLSocketTest {

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test(timeout = 60000, expected = IllegalStateException.class)
    public void testClientThrowsExceptionIfNodesAreUsingSSLButClientIsNot() throws Exception {
        Config serverConfig = new Config();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(getSslConfig());
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);
        factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        ClientConfig clientConfig = new ClientConfig();
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testClientOpenSSL_serverOpenSSL() throws Exception {
        assumeThatOpenSslIsSupported();
        test(true, true);
    }

    @Test
    public void testClientOpenSSL_serverSSLEngine() throws Exception {
        assumeThatOpenSslIsSupported();
        // for older java versions there can be incompatibilities between JSSE and OpenSSL: TLS protocol versions;
        // insufficient DH key length
        assumeJdk8OrNewer();
        test(true, false);
    }

    @Test
    public void testClientSSLEngine_serverOpenSSL() throws Exception {
        assumeThatOpenSslIsSupported();
        // for older java versions there can be incompatibilities between JSSE and OpenSSL: TLS protocol versions;
        // insufficient DH key length
        assumeJdk8OrNewer();
        test(false, true);
    }

    @Test
    public void testClientSSLEngine_serverSSLEngine() throws Exception {
        test(false, false);
    }

    public void test(boolean serverOpenSSL, boolean clientOpenSSL) throws Exception {
        Config serverConfig = new Config();
        // we need to provide TLS version explicitly in mixed scenarios on IBM Java
        boolean forceProtocol = TestEnvironmentUtil.isIbmJvm() && (serverOpenSSL ^ clientOpenSSL);

        Properties serverSslProperties = createSslProperties(serverOpenSSL);
        if (forceProtocol) {
            serverSslProperties.setProperty("protocol", "TLSv1.2");
        }
        SSLConfig serverSSLConfig = getSslConfig(serverSslProperties);
        serverSSLConfig.setEnabled(true);
        if (serverOpenSSL) {
            serverSSLConfig.setFactoryClassName(OpenSSLEngineFactory.class.getName());
        }
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(serverSSLConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);

        factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        // no need for keystore on client side
        Properties clientSslProps = createSslProperties(clientOpenSSL);
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE);
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE_PASSWORD);
        clientSslProps.remove(JAVAX_NET_SSL_KEY_FILE);
        if (forceProtocol) {
            clientSslProps.setProperty("protocol", "TLSv1.2");
        }
        ClientConfig clientConfig = new ClientConfig();
        SSLConfig clientSSLConfig = getSslConfig(clientSslProps);
        clientSSLConfig.setEnabled(true);
        if (clientOpenSSL) {
            clientSSLConfig.setFactoryClassName(OpenSSLEngineFactory.class.getName());
        }
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).setRedoOperation(true).setSSLConfig(clientSSLConfig);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        IMap<Object, Object> clientMap = client.getMap("test");

        int count = 10000;
        for (int i = 0; i < count; i++) {
            clientMap.put(i % 100, new byte[100 * 1024]);
        }
    }

    @Test
    public void testServerRequiresClientAuth_clientHaveKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig().setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "REQUIRED");
        Config serverConfig = new Config();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);

        HazelcastInstance hz1 = factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        Properties clientSslProps = createSslProperties();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test(expected = IllegalStateException.class)
    public void testServerRequiresClientAuth_clientDoesNotHaveKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig().setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "REQUIRED");
        Config serverConfig = new Config();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);


        factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        // no need for keystore on client side
        Properties clientSslProps = createSslProperties();
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE);
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE_PASSWORD);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        factory.newHazelcastClient(clientConfig);
    }

    @Test(expected = IllegalStateException.class)
    public void testServerRequiresClientAuth_clientHaveWrongKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig().setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "REQUIRED");
        Config serverConfig = new Config();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);

        factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        Properties clientSslProps = createSslProperties();
        clientSslProps.setProperty(JAVAX_NET_SSL_KEY_STORE, getWrongKeyStoreFilePath());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testOptionalClientAuth_clientHaveKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig().setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "OPTIONAL");
        Config serverConfig = new Config();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);

        HazelcastInstance hz1 = factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        Properties clientSslProps = createSslProperties();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test
    public void testOptionalClientAuth_clientHaveWrongKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig().setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "OPTIONAL");
        Config serverConfig = new Config();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);

        HazelcastInstance hz1 = factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        Properties clientSslProps = createSslProperties();
        clientSslProps.setProperty(JAVAX_NET_SSL_KEY_STORE, getWrongKeyStoreFilePath());
        clientSslProps.setProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, "123456");
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test
    public void testOptionalClientAuth_clientDoesNotHaveKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig().setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "OPTIONAL");
        Config serverConfig = new Config();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);

        HazelcastInstance hz1 = factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        // remove keystore on client properties side
        Properties clientSslProps = createSslProperties();
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE);
        clientSslProps.remove(JAVAX_NET_SSL_KEY_STORE_PASSWORD);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        IMap<Object, Object> clientMap = client.getMap("test");
        clientMap.put(1, 2);
        IMap<Object, Object> map = hz1.getMap("test");
        assertEquals(2, map.get(1));
    }

    @Test
    public void testMalformedKeystore_onClient() throws Exception {
        SSLConfig sslConfig = getSslConfig().setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "OPTIONAL");
        Config serverConfig = new Config();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);

        factory.newHazelcastInstance(serverConfig);

        // remove keystore on client properties side
        Properties clientSslProps = createSslProperties();
        clientSslProps.put(JAVAX_NET_SSL_KEY_STORE, getMalformedKeyStoreFilePath());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        try {
            factory.newHazelcastClient(clientConfig);
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
