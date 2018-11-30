package com.hazelcast.client.nio.ssl;

import com.hazelcast.TestEnvironmentUtil;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.OpenSSLEngineFactory;
import com.hazelcast.nio.ssl.SSLConnectionTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.TestEnvironmentUtil.assumeJavaVersionAtLeast;
import static com.hazelcast.TestEnvironmentUtil.assumeNoIbmJvm;
import static com.hazelcast.TestEnvironmentUtil.assumeThatNoJDK6;
import static com.hazelcast.TestEnvironmentUtil.copyTestResource;
import static com.hazelcast.TestEnvironmentUtil.isOpenSslSupported;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static java.util.Arrays.asList;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Functional TLS tests.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class})
public class TlsFunctionalTest {

    private static final String KEY_FILE_SERVER = "server.pem";
    private static final String CERT_FILE_SERVER = "server.crt";
    private static final String KEYSTORE_SERVER = "server.keystore";
    private static final String TRUSTSTORE_SERVER = "server.truststore";
    private static final String KEY_FILE_CLIENT = "client.pem";
    private static final String CERT_FILE_CLIENT = "client.crt";
    private static final String KEYSTORE_CLIENT = "client.keystore";
    private static final String TRUSTSTORE_CLIENT = "client.truststore";
    private static final String CERT_FILE_UNTRUSTED = "untrusted.crt";
    private static final String TRUSTSTORE_UNTRUSTED = "untrusted.truststore";
    private static final String TRUST_ALL = "all.crt";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @Parameter
    public boolean mutualAuthentication;

    @Parameter(value = 1)
    public boolean openSsl;

    @Parameters(name = "mutualAuthentication:{0} openSsl:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {false, false},
                {false, true},
                {true, false},
                {true, true},
        });
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Before
    public void before() {
        assumeTrue("OpenSSL enabled but not available", !openSsl || isOpenSslSupported());
    }

    /**
     * Case - Valid/trusted configuration.
     *
     * <pre>
     * Given: TLS is enabled on members and client
     * When: members has a valid and matching keystore + truststore configured; clients have matching SSL configured
     * Then: Members start successfully and form cluster, clients can connect
     * </pre>
     */
    @Test
    public void testValidConfiguration() throws IOException {
        Config config = createMemberConfig();
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
        factory.newHazelcastClient(createClientConfig()).getMap("test").put("a", "b");
    }

    /**
     * Case - Member with TLS, Client without TLS.
     *
     * <pre>
     * Given: TLS is enabled on members but not on clients
     * When: client tries to connect to a member
     * Then: connection fails
     * </pre>
     */
    @Test
    public void testOnlyMemberHasTls() throws IOException {
        factory.newHazelcastInstance(createMemberConfig());
        ClientConfig clientConfig = createClientConfig();
        clientConfig.getNetworkConfig().setSSLConfig(null);
        try {
            factory.newHazelcastClient(clientConfig);
            fail("Client should not be able to connect");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    /**
     * Case - Member without TLS, Client with TLS.
     *
     * <pre>
     * Given: TLS is enabled on clients but not on members
     * When: client tries to connect to a member
     * Then: connection fails
     * </pre>
     */
    @Test
    public void testOnlyClientHasTls() throws IOException {
        Config config = createMemberConfig();
        config.getNetworkConfig().setSSLConfig(null);
        factory.newHazelcastInstance(config);
        try {
            factory.newHazelcastClient(createClientConfig());
            fail("Client should not be able to connect");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    /**
     * Case - Untrusted configuration.
     *
     * <pre>
     * Given: TLS is enabled on members and client
     * When: certificates in member truststore doesn't cover the certificate in the keystore
     *       (i.e. member's certificate path is not validated against the truststore)
     * Then: Members start successfully, but they don't form a cluster, clients can't connect to any member
     * </pre>
     */
    @Test
    public void testUntrustedConfiguration() throws IOException {
        Config config = createMemberConfig();
        SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
        String trustPropertyName;
        String trustPropertyValue;
        if (openSsl) {
            trustPropertyName = "trustCertCollectionFile";
            trustPropertyValue = copyResource(CERT_FILE_UNTRUSTED).getAbsolutePath();
        } else {
            trustPropertyName = "trustStore";
            trustPropertyValue = copyResource(TRUSTSTORE_UNTRUSTED).getAbsolutePath();
        }
        sslConfig.setProperty(trustPropertyName, trustPropertyValue);

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(1, hz1, hz2);
        try {
            ClientConfig clientConfig = createClientConfig();
            sslConfig = clientConfig.getNetworkConfig().getSSLConfig();
            sslConfig.setProperty(trustPropertyName, trustPropertyValue);
            factory.newHazelcastClient(clientConfig);
            fail("Client should not be able to connect");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    /**
     * Case - Valid Ciphersuites.
     *
     * <pre>
     * Given: TLS is enabled
     * When: Ciphersuite configuration is provided with supported values
     * Then: Members start successfully and form cluster
     * </pre>
     */
    @Test
    public void testSupportedCipherSuiteNames() throws IOException {
        String cipherSuites =
                // OpenJDK
                "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,"
                        + "TLS_RSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,"
                        + "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA,"
                        // IBM Java
                        + "SSL_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,SSL_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,"
                        + "SSL_ECDHE_RSA_WITH_AES_128_CBC_SHA,SSL_ECDHE_RSA_WITH_AES_128_CBC_SHA256,"
                        + "SSL_RSA_WITH_AES_128_CBC_SHA,SSL_RSA_WITH_AES_128_CBC_SHA256";
        Config config = createMemberConfig();
        SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
        sslConfig.setProperty("ciphersuites", cipherSuites);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
        ClientConfig clientConfig = createClientConfig();
        sslConfig = clientConfig.getNetworkConfig().getSSLConfig();
        sslConfig.setProperty("ciphersuites", cipherSuites);
        factory.newHazelcastClient(clientConfig).getMap("test").put("a", "b");
    }

    /**
     * Case - Invalid Ciphersuites.
     *
     * <pre>
     * Given: TLS is enabled
     * When: Ciphersuite configuration is provided with unsupported values
     * Then: Member fails to start
     * </pre>
     */
    @Test(expected = HazelcastException.class)
    public void testUnsupportedCipherSuiteNames() throws IOException {
        Config config = createMemberConfig();
        SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
        sslConfig.setProperty("ciphersuites", "foo,bar");
        factory.newHazelcastInstance(config);
    }

    /**
     * Case - Invalid Ciphersuites in Client.
     *
     * <pre>
     * Given: TLS is enabled
     * When: Ciphersuite configuration is provided in client with unsupported values
     * Then: Client fails to start
     * </pre>
     */
    @Test(expected = HazelcastException.class)
    public void testUnsupportedClientCipherSuiteNames() throws IOException {
        factory.newHazelcastInstance(createMemberConfig());
        ClientConfig clientConfig = createClientConfig();
        SSLConfig sslConfig = clientConfig.getNetworkConfig().getSSLConfig();
        sslConfig.setProperty("ciphersuites", "foo,bar");
        factory.newHazelcastClient(clientConfig);
    }

    /**
     * Case - Valid Protocol.
     *
     * <pre>
     * Given: TLS is enabled
     * When: Protocol configuration is provided with supported value
     * Then: Members start successfully and form cluster
     * </pre>
     */
    @Test
    public void testSupportedProtocolName() throws IOException {
        Config config = createMemberConfig();
        config.getNetworkConfig().getSSLConfig().setProperty("protocol", "TLS");
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);

        ClientConfig clientConfig = createClientConfig();
        clientConfig.getNetworkConfig().getSSLConfig().setProperty("protocol", "TLS");
        factory.newHazelcastClient(clientConfig).getMap("test").put("a", "b");
    }

    /**
     * Smoke test for TLSv1.3 support in Java 11+.
     * See <a href="http://openjdk.java.net/jeps/332">http://openjdk.java.net/jeps/332</a>.
     *
     * <pre>
     * Given: Java runtime in version 11 or newer is used and OpenSSL is not enabled.
     * When: Protocol TLSv1.3 is configured in Hazelcast SSLConfig
     * Then: Members start successfully, form cluster and client is able to join.
     * </pre>
     */
    @Test
    public void testTLSv13onJava11() throws IOException {
        // once the https://github.com/netty/netty-tcnative/issues/256 is fixed, we can remove following assumption:
        assumeFalse(openSsl);
        assumeJavaVersionAtLeast(11);
        Config config = createMemberConfig();
        SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
        sslConfig.setProperty("protocol", "TLSv1.3");
        sslConfig.setProperty("ciphersuites", "TLS_AES_128_GCM_SHA256");
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);

        ClientConfig clientConfig = createClientConfig();
        SSLConfig clientSslConfig = clientConfig.getNetworkConfig().getSSLConfig();
        clientSslConfig.setProperty("protocol", "TLSv1.3");
        clientSslConfig.setProperty("ciphersuites", "TLS_AES_128_GCM_SHA256");
        factory.newHazelcastClient(clientConfig).getMap("test").put("a", "b");
    }

    /**
     * Case - Let's Encrypt certificate used.
     *
     * <pre>
     * Given: TLS is enabled.
     * When: 2 members are started with certificate signed by the Let's Encrypt CA.
     * Then: Members successfully form a cluster.
     * </pre>
     */
    @Test
    public void testLetsEncrypt() throws IOException {
        SSLConfig sslConfig = new SSLConfig().setEnabled(true);
        if (openSsl) {
            sslConfig.setFactoryClassName(OpenSSLEngineFactory.class.getName())
                    .setProperty("keyFile",
                            copyTestResource(SSLConnectionTest.class, tempFolder.getRoot(), "privkey.pem").getAbsolutePath())
                    .setProperty("keyCertChainFile",
                            copyTestResource(SSLConnectionTest.class, tempFolder.getRoot(), "fullchain.pem").getAbsolutePath())
                    .setProperty("trustCertCollectionFile",
                            copyTestResource(SSLConnectionTest.class, tempFolder.getRoot(), "chain.pem").getAbsolutePath());
        } else {
            File letsEncryptKeystore = copyTestResource(SSLConnectionTest.class, tempFolder.getRoot(), "letsencrypt.jks");
            sslConfig.setFactoryClassName(BasicSSLContextFactory.class.getName())
                    .setProperty("keyStore", letsEncryptKeystore.getAbsolutePath())
                    .setProperty("keyStorePassword", "123456")
                    .setProperty("trustStore", letsEncryptKeystore.getAbsolutePath())
                    .setProperty("trustStorePassword", "123456");
        }
        if (mutualAuthentication) {
            sslConfig.setProperty("mutualAuthentication", "REQUIRED");
        }

        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
    }

    /**
     * Case - no truststore is set - neither the keystore nor the JRE specific one (cacerts).
     *
     * <pre>
     * Given: TLS is enabled.
     * When: TrustStore is not configured within the SSL properties and 2 members are started.
     * Then: Members trust each other and they form a cluster via system trust store
     * </pre>
     */
    @Test
    public void testDefaultTruststore() throws IOException {
        assumeNoIbmJvm();
        assumeThatNoJDK6();
        SSLConfig sslConfig = new SSLConfig().setEnabled(true);
        setSignedKeyFiles(sslConfig);
        if (mutualAuthentication) {
            sslConfig.setProperty("mutualAuthentication", "REQUIRED");
        }

        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
    }

    private void setSignedKeyFiles(SSLConfig sslConfig) {
        if (openSsl) {
            sslConfig.setFactoryClassName(OpenSSLEngineFactory.class.getName())
                    .setProperty("keyFile",
                            copyTestResource(SSLConnectionTest.class, tempFolder.getRoot(), "privkey.pem").getAbsolutePath())
                    .setProperty("keyCertChainFile",
                            copyTestResource(SSLConnectionTest.class, tempFolder.getRoot(), "fullchain.pem").getAbsolutePath());
        } else {
            File letsEncryptKeystore = copyTestResource(SSLConnectionTest.class, tempFolder.getRoot(), "letsencrypt.jks");
            sslConfig.setFactoryClassName(BasicSSLContextFactory.class.getName())
                    .setProperty("keyStore", letsEncryptKeystore.getAbsolutePath())
                    .setProperty("keyStorePassword", "123456");
        }
    }

    /**
     * Case - no truststore is set - neither the keystore nor the JRE specific one (cacerts).
     *
     * <pre>
     * Given: TLS is enabled.
     * When: TrustStore is not configured within the SSL properties and a member and a client started.
     * Then: Client trusts member and joins via system trust store
     * </pre>
     */
    @Test
    public void testDefaultTruststore_client() throws IOException {
        assumeNoIbmJvm();
        assumeThatNoJDK6();
        SSLConfig sslConfig = new SSLConfig().setEnabled(true);
        SSLConfig clientSSLConfig = new SSLConfig().setEnabled(true);
        setSignedKeyFiles(sslConfig);
        if (mutualAuthentication) {
            sslConfig.setProperty("mutualAuthentication", "REQUIRED");
            setSignedKeyFiles(clientSSLConfig);
        }

        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);
        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        clientConfig.getNetworkConfig().setSSLConfig(clientSSLConfig);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        assertClusterSize(1, hz, client);
    }

    /**
     * Case - JRE default truststore is used (by explicit path declaration to cacerts).
     *
     * <pre>
     * Given: TLS is enabled and a Let's Encrypt issued certificate is used.
     * When: TrustStore is configured using {@code ${java.home}/lib/security/cacerts} truststure and 2 members are started.
     * Then: Members trust each other and they form a cluster.
     * </pre>
     */
    @Test
    public void testDefaultTruststore_configuredExplicitly() throws IOException {
        // older Java versions don't have the Let's Encrypt CA certificate in their truststores
        assumeJavaVersionAtLeast(8);
        assumeFalse(openSsl && TestEnvironmentUtil.isIbmJvm());
        File letsEncryptKeystore = copyTestResource(SSLConnectionTest.class, tempFolder.getRoot(), "letsencrypt.jks");
        String xml = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n"
                + "    <network>\n"
                + "        <ssl enabled=\"true\">\r\n"
                + "          <properties>\r\n"
                + "            <property name=\"keyStore\">" + letsEncryptKeystore.getAbsolutePath() + "</property>\r\n"
                + "            <property name=\"keyStorePassword\">123456</property>\r\n"
                + "            <property name=\"trustStore\">${java.home}/lib/security/cacerts</property>\r\n"
                + "          </properties>\r\n"
                + "        </ssl>\r\n"
                + "    </network>\n"
                + "</hazelcast>\n";
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config config = configBuilder.build();
        config.getNetworkConfig().getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);
        if (mutualAuthentication) {
            config.getNetworkConfig().getSSLConfig().setProperty("mutualAuthentication", "REQUIRED");
        }
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
    }

    /**
     * Case - Invalid Protocol.
     *
     * <pre>
     * Given: TLS is enabled
     * When: Protocol configuration is provided with unsupported value
     * Then: Member fails to start
     * </pre>
     */
    @Test(expected = HazelcastException.class)
    public void testUnsupportedProtocolName() throws IOException {
        Config config = createMemberConfig();
        SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
        sslConfig.setProperty("protocol", "hazelcast");
        factory.newHazelcastInstance(config);
    }

    /**
     * Case - Invalid Protocol in Client.
     *
     * <pre>
     * Given: TLS is enabled
     * When: Protocol configuration is provided in client with an unsupported value
     * Then: Client fails to start
     * </pre>
     */
    @Test(expected = HazelcastException.class)
    public void testUnsupportedClientProtocolName() throws IOException {
        factory.newHazelcastInstance(createMemberConfig());
        ClientConfig clientConfig = createClientConfig();
        SSLConfig sslConfig = clientConfig.getNetworkConfig().getSSLConfig();
        sslConfig.setProperty("protocol", "hazelcast");
        factory.newHazelcastClient(clientConfig);
    }

    private Config createMemberConfig() throws IOException {
        SSLConfig sslConfig = new SSLConfig().setEnabled(true);
        if (openSsl) {
            sslConfig.setFactoryClassName(OpenSSLEngineFactory.class.getName())
            .setProperty("keyFile", copyResource(KEY_FILE_SERVER).getAbsolutePath())
            .setProperty("keyCertChainFile", copyResource(CERT_FILE_SERVER).getAbsolutePath())
            .setProperty("trustCertCollectionFile", copyResource(TRUST_ALL).getAbsolutePath());
        } else {
            sslConfig
            .setProperty("keyStore", copyResource(KEYSTORE_SERVER).getAbsolutePath())
            .setProperty("keyStorePassword", "123456")
            .setProperty("trustStore", copyResource(TRUSTSTORE_SERVER).getAbsolutePath())
            .setProperty("trustStorePassword", "123456");
        }
        if (mutualAuthentication) {
            sslConfig.setProperty("mutualAuthentication", "REQUIRED");
        }

        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);
        return config;
    }

    private ClientConfig createClientConfig() throws IOException {
        SSLConfig sslConfig = new SSLConfig().setEnabled(true);
        if (openSsl) {
            sslConfig.setFactoryClassName(OpenSSLEngineFactory.class.getName())
            .setProperty("trustCertCollectionFile", copyResource(CERT_FILE_SERVER).getAbsolutePath());
            if (mutualAuthentication) {
                sslConfig.setProperty("keyFile", copyResource(KEY_FILE_CLIENT).getAbsolutePath())
                .setProperty("keyCertChainFile", copyResource(CERT_FILE_CLIENT).getAbsolutePath());
            }
        } else {
            sslConfig
            .setProperty("trustStore", copyResource(TRUSTSTORE_CLIENT).getAbsolutePath())
            .setProperty("trustStorePassword", "123456");
            if (mutualAuthentication) {
                sslConfig.setProperty("keyStore", copyResource(KEYSTORE_CLIENT).getAbsolutePath());
                sslConfig.setProperty("keyStorePassword", "123456");
            }
        }

        ClientConfig config = new ClientConfig();
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.setConnectionTimeout(15000);
        return config;
    }

    /**
     * Copies a resource file from current package to location denoted by given {@link java.io.File} instance.
     */
    private File copyResource(String resourceName) throws IOException {
        return copyTestResource(getClass(), tempFolder.getRoot(), resourceName);
    }
}
