package com.hazelcast.client.nio.ssl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.OpenSSLEngineFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import static com.hazelcast.TestEnvironmentUtil.isOpenSslSupported;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.nio.IOUtil.copy;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Functional TLS tests.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class})
public class TlsFunctionalTest {

    private static final String KEYSTORE_SERVER = "server.keystore";
    private static final String TRUSTSTORE_SERVER = "server.truststore";
    private static final String KEYSTORE_CLIENT = "client.keystore";
    private static final String TRUSTSTORE_CLIENT = "client.truststore";
    private static final String TRUSTSTORE_UNTRUSTED = "untrusted.truststore";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private final ILogger logger = Logger.getLogger(TlsFunctionalTest.class);

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

    @AfterClass
    public static void shutDownAll() {
        HazelcastClient.shutdownAll();
        HazelcastInstanceFactory.terminateAll();
    }

    @Before
    public void before() {
        assumeTrue("OpenSSL enabled but not available", !openSsl || isOpenSslSupported());
        shutDownAll();
    }

    /**
     * Case - Valid/trusted configuration.
     * <pre>
     * Given: TLS is enabled on members and client
     * When: members has a valid and matching keystore + truststore configured; clients have matching SSL configured
     * Then: Members start successfully and form cluster, clients can connect
     * </pre>
     */
    @Test
    public void testValidConfiguration() throws IOException {
        Config config = createMemberConfig();
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
        HazelcastClient.newHazelcastClient(createClientConfig()).getMap("test").put("a", "b");
    }

    /**
     * Case - Member with TLS, Client without TLS.
     * <pre>
     * Given: TLS is enabled on members but not on clients
     * When: client tries to connect to a member
     * Then: connection fails
     * </pre>
     */
    @Test
    public void testOnlyMemberHasTls() throws IOException {
        Hazelcast.newHazelcastInstance(createMemberConfig());
        ClientConfig clientConfig = createClientConfig();
        clientConfig.getNetworkConfig().setSSLConfig(null);
        try {
            HazelcastClient.newHazelcastClient(clientConfig);
            fail("Client should not be able to connect");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    /**
     * Case - Member without TLS, Client with TLS.
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
        Hazelcast.newHazelcastInstance(config);
        try {
            HazelcastClient.newHazelcastClient(createClientConfig());
            fail("Client should not be able to connect");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    /**
     * Case - Untrusted configuration.
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
        String untrustedTruststore = copyResource(TRUSTSTORE_UNTRUSTED).getAbsolutePath();
        sslConfig.setProperty("trustStore", untrustedTruststore);

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(1, hz1, hz2);
        try {
            ClientConfig clientConfig = createClientConfig();
            sslConfig = clientConfig.getNetworkConfig().getSSLConfig();
            sslConfig.setProperty("trustStore", untrustedTruststore);
            HazelcastClient.newHazelcastClient(clientConfig);
            fail("Client should not be able to connect");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    /**
     * Case - Valid Ciphersuites.
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
                + "SSL_RSA_WITH_AES_128_CBC_SHA,SSL_RSA_WITH_AES_128_CBC_SHA256"
                ;
        Config config = createMemberConfig();
        SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
        sslConfig.setProperty("ciphersuites", cipherSuites);
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
        ClientConfig clientConfig = createClientConfig();
        sslConfig = clientConfig.getNetworkConfig().getSSLConfig();
        sslConfig.setProperty("ciphersuites", cipherSuites);
        HazelcastClient.newHazelcastClient(clientConfig).getMap("test").put("a", "b");
    }

    /**
     * Case - Invalid Ciphersuites.
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
        Hazelcast.newHazelcastInstance(config);
    }

    /**
     * Case - Invalid Ciphersuites in Client.
     * <pre>
     * Given: TLS is enabled
     * When: Ciphersuite configuration is provided in client with unsupported values
     * Then: Client fails to start
     * </pre>
     */
    @Test(expected = HazelcastException.class)
    public void testUnsupportedClientCipherSuiteNames() throws IOException {
        Hazelcast.newHazelcastInstance(createMemberConfig());
        ClientConfig clientConfig = createClientConfig();
        SSLConfig sslConfig = clientConfig.getNetworkConfig().getSSLConfig();
        sslConfig.setProperty("ciphersuites", "foo,bar");
        HazelcastClient.newHazelcastClient(clientConfig);
    }

    /**
     * Case - Valid Protocol.
     * <pre>
     * Given: TLS is enabled
     * When: Protocol configuration is provided with supported value
     * Then: Members start successfully and form cluster
     * </pre>
     */
    @Test
    public void testSupportedProtocolName() throws IOException {
        Config config = createMemberConfig();
        config.getNetworkConfig().getSSLConfig()
                .setProperty("protocol", "TLS");
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);

        ClientConfig clientConfig = createClientConfig();
        clientConfig.getNetworkConfig().getSSLConfig()
                .setProperty("protocol", "TLS");
        HazelcastClient.newHazelcastClient(clientConfig).getMap("test").put("a", "b");
    }

    /**
     * Case - Invalid Protocol.
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
        Hazelcast.newHazelcastInstance(config);
    }

    /**
     * Case - Invalid Protocol in Client.
     * <pre>
     * Given: TLS is enabled
     * When: Protocol configuration is provided in client with an unsupported value
     * Then: Client fails to start
     * </pre>
     */
    @Test(expected = HazelcastException.class)
    public void testUnsupportedClientProtocolName() throws IOException {
        Hazelcast.newHazelcastInstance(createMemberConfig());
        ClientConfig clientConfig = createClientConfig();
        SSLConfig sslConfig = clientConfig.getNetworkConfig().getSSLConfig();
        sslConfig.setProperty("protocol", "hazelcast");
        HazelcastClient.newHazelcastClient(clientConfig);
    }

    private Config createMemberConfig() throws IOException {
        SSLConfig sslConfig = new SSLConfig()
                .setEnabled(true)
                .setFactoryClassName((openSsl ? OpenSSLEngineFactory.class : BasicSSLContextFactory.class).getName())
                .setProperty("keyStore", copyResource(KEYSTORE_SERVER).getAbsolutePath())
                .setProperty("keyStorePassword", "123456")
                .setProperty("trustStore", copyResource(TRUSTSTORE_SERVER).getAbsolutePath())
                .setProperty("trustStorePassword", "123456");
        if (mutualAuthentication) {
            sslConfig.setProperty("mutualAuthentication", "REQUIRED");
        }

        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig()
                .setSSLConfig(sslConfig);
        networkConfig.getJoin().getMulticastConfig()
                .setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1");
        return config;
    }

    private ClientConfig createClientConfig() throws IOException {
        SSLConfig sslConfig = new SSLConfig()
                .setEnabled(true)
                .setFactoryClassName((openSsl ? OpenSSLEngineFactory.class : BasicSSLContextFactory.class).getName())
                .setProperty("trustStore", copyResource(TRUSTSTORE_CLIENT).getAbsolutePath())
                .setProperty("trustStorePassword", "123456");
        if (mutualAuthentication) {
            sslConfig.setProperty("keyStore", copyResource(KEYSTORE_CLIENT).getAbsolutePath());
            sslConfig.setProperty("keyStorePassword", "123456");
        }

        ClientConfig config = new ClientConfig();
        config.getNetworkConfig()
                .setSSLConfig(sslConfig)
                .addAddress("127.0.0.1");
        return config;
    }

    /**
     * Copies a resource file from current package to location denoted by given {@link java.io.File} instance.
     */
    private File copyResource(String resourceName) throws IOException {
        File targetFile = new File(tempFolder.getRoot(), resourceName);
        if (!targetFile.exists()) {
            assertTrue(targetFile.createNewFile());
            logger.info("Copying test resource to file " + targetFile.getAbsolutePath());
            InputStream is = null;
            try {
                is = TlsFunctionalTest.class.getResourceAsStream(resourceName);
                copy(is, targetFile);
            } finally {
                closeResource(is);
            }
        }
        return targetFile;
    }
}
