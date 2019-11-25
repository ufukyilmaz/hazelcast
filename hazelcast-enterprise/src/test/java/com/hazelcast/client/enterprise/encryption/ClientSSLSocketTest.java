package com.hazelcast.client.enterprise.encryption;

import com.hazelcast.TestEnvironmentUtil;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.nio.ssl.OpenSSLEngineFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.TestEnvironmentUtil.assumeJavaVersionLessThan;
import static com.hazelcast.TestEnvironmentUtil.assumeThatOpenSslIsSupported;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_FILE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_MUTUAL_AUTHENTICATION;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.createSslProperties;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getOrCreateTempFile;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.keyStore;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.keyStore2;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.malformedKeystore;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.trustStoreTwoCertificates;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.wrongKeyStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientSSLSocketTest extends ClientTestSupport {

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test(timeout = 60000, expected = IllegalStateException.class)
    public void testClientThrowsExceptionIfNodesAreUsingSSLButClientIsNot() throws Exception {
        Config serverConfig = smallInstanceConfig();
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
        test(true, false);
    }

    @Test
    public void testClientSSLEngine_serverOpenSSL() throws Exception {
        assumeThatOpenSslIsSupported();
        // for older java versions there can be incompatibilities between JSSE and OpenSSL: TLS protocol versions;
        // insufficient DH key length
        test(false, true);
    }

    @Test
    public void testClientSSLEngine_serverSSLEngine() throws Exception {
        test(false, false);
    }

    public void test(boolean clientOpenSSL, boolean serverOpenSSL) throws Exception {
        Config serverConfig = smallInstanceConfig();
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
        Config serverConfig = smallInstanceConfig();
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
        Config serverConfig = smallInstanceConfig();
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
        Config serverConfig = smallInstanceConfig();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);

        factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        Properties clientSslProps = createSslProperties();
        clientSslProps.setProperty(JAVAX_NET_SSL_KEY_STORE, getOrCreateTempFile(wrongKeyStore));
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testOptionalClientAuth_clientHaveKeystore() throws Exception {
        SSLConfig sslConfig = getSslConfig().setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "OPTIONAL");
        Config serverConfig = smallInstanceConfig();
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
        // see. https://github.com/hazelcast/hazelcast-enterprise/issues/2710#issuecomment-475596023
        // The test is only valid for TLS 1.2 (i.e. JDK < 11)
        assumeJavaVersionLessThan(11);

        SSLConfig sslConfig = getSslConfig().setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "OPTIONAL");
        Config serverConfig = smallInstanceConfig();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);

        HazelcastInstance hz1 = factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        Properties clientSslProps = createSslProperties();
        clientSslProps.setProperty(JAVAX_NET_SSL_KEY_STORE, getOrCreateTempFile(wrongKeyStore));
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
        Config serverConfig = smallInstanceConfig();
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

    @Ignore("See https://github.com/hazelcast/hazelcast-enterprise/issues/2736")
    @Test
    public void testMalformedKeystore_onClient() throws Exception {
        SSLConfig sslConfig = getSslConfig().setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "OPTIONAL");
        Config serverConfig = smallInstanceConfig();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);

        factory.newHazelcastInstance(serverConfig);

        // remove keystore on client properties side
        Properties clientSslProps = createSslProperties();
        clientSslProps.put(JAVAX_NET_SSL_KEY_STORE, getOrCreateTempFile(malformedKeystore));
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

    @Test
    public void testTwoServers_withDifferentKeys() {
        Properties sslProperties1 = createSslPropertiesTrustTwoCertificate();
        sslProperties1.setProperty(JAVAX_NET_SSL_KEY_STORE, getOrCreateTempFile(keyStore));
        sslProperties1.setProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, "123456");
        Config config1 = getConfig(sslProperties1);
        HazelcastInstance h1 = factory.newHazelcastInstance(config1);

        Properties sslProperties2 = createSslPropertiesTrustTwoCertificate();
        sslProperties2.setProperty(JAVAX_NET_SSL_KEY_STORE, getOrCreateTempFile(keyStore2));
        sslProperties2.setProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, "123456");
        Config config2 = getConfig(sslProperties2);
        HazelcastInstance h2 = factory.newHazelcastInstance(config2);

        assertClusterSize(2, h1, h2);

        Properties clientSslProps = createSslPropertiesTrustTwoCertificate();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).setRedoOperation(true)
                .setSSLConfig(getSslConfig(clientSslProps));

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        makeSureConnectedToServers(client, 2);
    }

    private static Properties createSslPropertiesTrustTwoCertificate() {
        Properties props = new Properties();
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE, getOrCreateTempFile(trustStoreTwoCertificates));
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, "123456");
        return props;
    }

    private Config getConfig(Properties sslProperties) {
        SSLConfig sslConfig = getSslConfig(sslProperties);
        Config serverConfig = smallInstanceConfig();
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);
        return serverConfig;
    }


    private static SSLConfig getSslConfig() throws Exception {
        return getSslConfig(createSslProperties());
    }

    private static SSLConfig getSslConfig(Properties sslProps) {
        return new SSLConfig().setEnabled(true).setProperties(sslProps);
    }
}
