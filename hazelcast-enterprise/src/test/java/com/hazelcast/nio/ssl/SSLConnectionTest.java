package com.hazelcast.nio.ssl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.cluster.Member;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.JAVA_NET_SSL_PREFIX;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.createSslProperties;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getOrCreateTempFile;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.keyStore;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.keyStore2;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.trustStore;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class})
public class SSLConnectionTest {

    @Parameterized.Parameters(name = "advancedNetworking:{0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Parameterized.Parameter
    public boolean advancedNetworking;


    private final ILogger logger = Logger.getLogger(getClass());

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    @BeforeClass
    @AfterClass
    public static void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test(timeout = 1000 * 180)
    public void testNodes() {
        Config config = getConfig(TestKeyStoreUtil.createSslProperties());

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        HazelcastInstance h3 = factory.newHazelcastInstance(config);

        assertClusterSize(3, h1, h2, h3);

        TestUtil.warmUpPartitions(h1, h2, h3);
        Member owner1 = h1.getPartitionService().getPartition(0).getOwner();
        Member owner2 = h2.getPartitionService().getPartition(0).getOwner();
        Member owner3 = h3.getPartitionService().getPartition(0).getOwner();
        assertEquals(owner1, owner2);
        assertEquals(owner1, owner3);

        String name = "ssl-test";
        int count = 128;
        IMap<Integer, byte[]> map1 = h1.getMap(name);
        for (int i = 1; i < count; i++) {
            map1.put(i, new byte[1024 * i]);
        }

        IMap<Integer, byte[]> map2 = h2.getMap(name);
        for (int i = 1; i < count; i++) {
            byte[] bytes = map2.get(i);
            assertEquals(i * 1024, bytes.length);
        }

        IMap<Integer, byte[]> map3 = h3.getMap(name);
        for (int i = 1; i < count; i++) {
            byte[] bytes = map3.get(i);
            assertEquals(i * 1024, bytes.length);
        }
    }

    @Test(timeout = 1000 * 600)
    public void testPutAndGetAlwaysGoesToWire() {
        Config config = getConfig(TestKeyStoreUtil.createSslProperties());

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);

        assertClusterSize(2, h1, h2);

        TestUtil.warmUpPartitions(h1, h2);
        Member owner1 = h1.getPartitionService().getPartition(0).getOwner();
        Member owner2 = h2.getPartitionService().getPartition(0).getOwner();
        assertEquals(owner1, owner2);


        String name = "ssl-test";

        IMap<String, byte[]> map1 = h1.getMap(name);
        final int count = 256;
        for (int i = 1; i <= count; i++) {
            final String key = HazelcastTestSupport.generateKeyOwnedBy(h2);
            map1.put(key, new byte[1024 * i]);
            byte[] bytes = map1.get(key);
            assertEquals(i * 1024, bytes.length);

        }
        assertEquals(count, map1.size());
    }

    /**
     * Tests that 2 nodes form cluster if their SSL configurations has the same (single) ciphersuite name configured.
     */
    @Test
    public void testOneCipherSuite() throws GeneralSecurityException {
        List<String> supportedCipherSuites = Arrays.asList(getSupportedCipherSuites());
        logger.info("Supported ciphersuites: " + supportedCipherSuites);
        String[] knownCs = {"SSL_DHE_RSA_WITH_3DES_EDE_CBC_SHA", "SSL_DHE_RSA_WITH_3DES_EDE_CBC_SHA",
                "TLS_RSA_WITH_AES_128_CBC_SHA", "SSL_DHE_RSA_WITH_3DES_EDE_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA256",
        };
        int idx = 0;
        while (idx < knownCs.length) {
            if (supportedCipherSuites.contains(knownCs[idx])) {
                break;
            }
            idx++;
        }
        assumeTrue(idx < knownCs.length);
        logger.info("Ciphersuite selected in testOneCipherSuite(): " + knownCs[idx]);
        Config config = createConfigWithSslProperty("ciphersuites", knownCs[idx]);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);

        assertClusterSize(2, h1, h2);
    }

    /**
     * Tests that 2 nodes form cluster if their SSL configurations has all supported ciphersuites configured.
     */
    @Test
    public void testAllCipherSuites() throws GeneralSecurityException {
        String allSuites = cropFirstAndLastChar(Arrays.toString(getSupportedCipherSuites()));
        HazelcastInstance h1 = factory.newHazelcastInstance(createConfigWithSslProperty("ciphersuites", allSuites));
        HazelcastInstance h2 = factory
                .newHazelcastInstance(createConfigWithSslProperty(JAVA_NET_SSL_PREFIX + "ciphersuites", allSuites));

        assertClusterSize(2, h1, h2);
    }

    /**
     * Tests that 2 nodes don't form cluster if their SSL configurations have distinct sets of ciphersuites.
     */
    @Test
    public void testDifferentCipherSuites() throws GeneralSecurityException {
        List<String> supportedCipherSuites = Arrays.asList(getSupportedCipherSuites());
        assumeTrue("We need at least 2 supported ciphersuites for this test", supportedCipherSuites.size() > 1);
        int halfIdx = supportedCipherSuites.size() / 2;
        String firstHalf = cropFirstAndLastChar(supportedCipherSuites.subList(0, halfIdx).toString());
        String secondHalf = cropFirstAndLastChar(supportedCipherSuites.subList(halfIdx, supportedCipherSuites.size()).toString());

        HazelcastInstance h1 = factory
                .newHazelcastInstance(createConfigWithSslProperty("ciphersuites", firstHalf));
        try {
            factory.newHazelcastInstance(createConfigWithSslProperty(JAVA_NET_SSL_PREFIX + "ciphersuites", secondHalf));
            fail("Node should fail to start.");
        } catch (IllegalStateException e) {
            ignore(e);
        }

        assertClusterSize(1, h1);
    }

    /**
     * Tests that 2 nodes form cluster if their SSL configurations contains the same set of supported and unsupported ciphersuites.
     */
    @Test
    public void testUnknownCipherSuites() throws GeneralSecurityException {
        String allSuites = Arrays.toString(getSupportedCipherSuites());
        Config config = createConfigWithSslProperty("ciphersuites",
                "FOO_BAR," + cropFirstAndLastChar(allSuites) + ",HAZELCAST");

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);

        assertClusterSize(2, h1, h2);
    }

    /**
     * Tests that node doesn't start when SSL configurations contains only unsupported ciphersuite names.
     */
    @Test(expected = InvalidConfigurationException.class)
    public void testUnsupportedCipherSuiteNames() {
        factory.newHazelcastInstance(createConfigWithSslProperty("ciphersuites", "foo,bar"));
        factory.newHazelcastInstance(createConfigWithSslProperty("ciphersuites", "foo,bar"));
    }

    /**
     * Tests that a node doesn't start if its SSL configurations contains empty ("") ciphersuites property value.
     */
    @Test(expected = InvalidConfigurationException.class)
    public void testEmptyCipherSuiteProperty() {
        factory.newHazelcastInstance(createConfigWithSslProperty("ciphersuites", ""));
        factory.newHazelcastInstance(createConfigWithSslProperty("ciphersuites", ""));
    }

    /**
     * Tests that 2 nodes form cluster if their SSL configurations have the same protocol property value.
     */
    @Test
    public void testTlsProtocol() {
        HazelcastInstance h1 = factory.newHazelcastInstance(createConfigWithSslProperty("protocol", "TLS"));
        HazelcastInstance h2 = factory.newHazelcastInstance(createConfigWithSslProperty("protocol", "TLS"));

        assertClusterSize(2, h1, h2);
    }

    /**
     * Tests that 2 nodes form cluster if their SSL configurations have the same protocol property value, but the property name
     * differs in prefix.
     */
    @Test
    public void testProtocolPrefix() throws GeneralSecurityException {
        String supportedTls = null;
        for (String protocol : getSupportedProtocols()) {
            if (protocol.startsWith("TLSv1")) {
                supportedTls = protocol;
                break;
            }
        }
        assumeNotNull("At least 1 supported TLS protocol version is necessary for this test", supportedTls);
        HazelcastInstance h1 = factory.newHazelcastInstance(createConfigWithSslProperty("protocol", supportedTls));
        HazelcastInstance h2 = factory.newHazelcastInstance(createConfigWithSslProperty(JAVA_NET_SSL_PREFIX + "protocol", supportedTls));

        assertClusterSize(2, h1, h2);
    }

    /**
     * Tests that 2 nodes don't form cluster if their SSL configurations have different TLS versions in protocol property value.
     */
    @Test
    public void testDifferentTlsProtocols() throws GeneralSecurityException {
        List<String> supportedTls = new ArrayList<String>();
        for (String protocol : getSupportedProtocols()) {
            if (protocol.startsWith("TLSv1")) {
                supportedTls.add(protocol);
            }
        }
        assumeTrue("At least 2 supported TLS protocol versions necessary for this test", supportedTls.size() > 1);

        HazelcastInstance h1 = factory.newHazelcastInstance(createConfigWithSslProperty("protocol", supportedTls.get(0)));
        try {
            factory.newHazelcastInstance(createConfigWithSslProperty(JAVA_NET_SSL_PREFIX + "protocol", supportedTls.get(1)));
            fail("Node should fail to start");
        } catch (IllegalStateException e) {
            ignore(e);
        }

        assertClusterSize(1, h1);
    }

    @Test(timeout = 1000 * 180)
    public void testTwoNodes_withDifferentKeys() {
        Properties sslProperties1 = createSslProperties();
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

        TestUtil.warmUpPartitions(h1, h2);
        Member owner1 = h1.getPartitionService().getPartition(0).getOwner();
        Member owner2 = h2.getPartitionService().getPartition(0).getOwner();
        assertEquals(owner1, owner2);

        String name = "ssl-test";
        int count = 128;
        IMap<Integer, byte[]> map1 = h1.getMap(name);
        for (int i = 1; i < count; i++) {
            map1.put(i, new byte[1024 * i]);
        }

        IMap<Integer, byte[]> map2 = h2.getMap(name);
        for (int i = 1; i < count; i++) {
            byte[] bytes = map2.get(i);
            assertEquals(i * 1024, bytes.length);
        }
    }

    private static Properties createSslPropertiesTrustTwoCertificate() {
        Properties props = new Properties();
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE, getOrCreateTempFile(trustStore));
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, "123456");
        return props;
    }

    private Config getConfig(Properties sslProperties) {
        Config config = smallInstanceConfig()
            .setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1")
            .setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "5");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);

        if (advancedNetworking) {
            config.getAdvancedNetworkConfig().setEnabled(true);
            ServerSocketEndpointConfig memberEndpoint
                    = (ServerSocketEndpointConfig) config.getAdvancedNetworkConfig()
                                                         .getEndpointConfigs().get(MEMBER);
            memberEndpoint.setSSLConfig(
                    new SSLConfig().setEnabled(true)
                                   .setProperties(sslProperties));

            config.getAdvancedNetworkConfig().getJoin()
                  .getTcpIpConfig().setEnabled(true)
                                   .setConnectionTimeoutSeconds(30);
        } else {
            config.getNetworkConfig().setSSLConfig(
                    new SSLConfig().setEnabled(true)
                                   .setProperties(sslProperties))
                                    .getJoin().getTcpIpConfig().setEnabled(true)
                                                               .setConnectionTimeoutSeconds(30);
        }

        return config;
    }

    private Config createConfigWithSslProperty(String propertyName, String propertyValue) {
        Properties props = TestKeyStoreUtil.createSslProperties();
        props.setProperty(propertyName, propertyValue);

        SSLConfig sslConfig = new SSLConfig()
                .setEnabled(true)
                .setProperties(props);

        Config config = smallInstanceConfig()
            .setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1")
            .setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "5");
        if (advancedNetworking) {
            config.getAdvancedNetworkConfig().setEnabled(true);
            ServerSocketEndpointConfig memberEndpoint =
                    (ServerSocketEndpointConfig) config.getAdvancedNetworkConfig().getEndpointConfigs().get(MEMBER);
            memberEndpoint.setSSLConfig(sslConfig);
            config.getAdvancedNetworkConfig().getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(30);
        } else {
            config.getNetworkConfig().setSSLConfig(sslConfig).getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(30);
        }

        return config;
    }

    private String[] getSupportedCipherSuites() throws GeneralSecurityException {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, null);
        String[] supportedCipherSuites = sslContext.getSupportedSSLParameters().getCipherSuites();
        assumeTrue("At least one supported cipher suite must be present for the tests", supportedCipherSuites.length > 0);
        return supportedCipherSuites;
    }

    private String[] getSupportedProtocols() throws GeneralSecurityException {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, null);
        String[] supportedProtocols = sslContext.getSupportedSSLParameters().getProtocols();
        assumeTrue("At least one supported cipher suite must be present for the tests", supportedProtocols.length > 0);
        return supportedProtocols;
    }

    private String cropFirstAndLastChar(String input) {
        return input.substring(1, input.length() - 1);
    }
}
