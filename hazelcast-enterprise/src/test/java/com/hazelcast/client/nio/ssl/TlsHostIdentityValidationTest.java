package com.hazelcast.client.nio.ssl;

import static com.hazelcast.TestEnvironmentUtil.copyTestResource;
import static com.hazelcast.TestEnvironmentUtil.isOpenSslSupported;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.Accessors.getAddress;
import static java.util.Arrays.asList;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.OpenSSLEngineFactory;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.annotation.SlowTest;

/**
 * Integration tests for TLS host identity validation. It uses pre-generated key material which was created by executing the
 * {@code createKeyMaterial.sh} script (you can find it in resources).
 * <p>
 * Given: Pregenerated key material is copied to a temporary folder. The default host validation is enabled for both members and
 * clients.
 * </p>
 *
 * @see TlsHostValidationSmokeTest
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({ SlowTest.class })
public class TlsHostIdentityValidationTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private static final String KEYSTORE_SAN_LOOPBACK = "tls-host-loopback-san.p12";
    private static final String KEYSTORE_SAN_LOOPBACK_DNS = "tls-host-loopback-san-dns.p12";
    private static final String KEYSTORE_SAN_FOREIGN = "tls-host-not-our-san.p12";
    private static final String KEYSTORE_CN_LOOPBACK = "tls-host-loopback-san.p12";
    private static final String KEYSTORE_NO_HOSTNAME = "tls-host-no-entry.p12";
    private static final String TRUSTSTORE = "truststore.p12";

    private static final int CONNECTION_TIMEOUT_SECONDS = 15;
    private static final String CLUSTER_JOIN_MAX_SECONDS = "15";

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @Parameter
    public boolean mutualAuthentication;

    @Parameter(value = 1)
    public boolean openSsl;

    @Parameters(name = "mutualAuthentication:{0}  openSsl:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
            {false, false},
            {false, true},
            {true, false},
            {true, true},
        });
    }

    private File trustStoreFile;

    @Before
    public void before() throws IOException {
        assumeTrue("OpenSSL enabled but not available", !openSsl || isOpenSslSupported());
        trustStoreFile = copyResource(TRUSTSTORE);
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    /**
     * <pre>
     * When: 2 members share one keystore with correct SAN entry (for loopback)
     * Then: the members connects and form 2 node cluster
     * </pre>
     */
    @Test
    public void testSanLoopback() throws IOException {
        File keystoreFile = copyResource(KEYSTORE_SAN_LOOPBACK);
        Config config = createMemberConfig(keystoreFile);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
    }

    /**
     * <pre>
     * When: one member uses keystore with correct SAN DNS "localhost" and client connects to it
     * Then: the connection succeeds
     * </pre>
     */
    @Test
    public void testSanLoopbackDnsOnly() throws IOException {
        assumeLocalhostResolvesTo_127_0_0_1();
        File keystoreFile = copyResource(KEYSTORE_SAN_LOOPBACK_DNS);
        Config config = createMemberConfig(keystoreFile);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        ClientConfig clientConfig = createClientConfig(keystoreFile)
                .setClusterName(config.getClusterName());
        int clientPort = getAddress(hz, EndpointQualifier.CLIENT).getPort();
        clientConfig.getNetworkConfig().addAddress("localhost:" + clientPort);
        HazelcastClient.newHazelcastClient(clientConfig);
    }

    /**
     * <pre>
     * When: 2 members share one keystore with incorrect SAN entry (single IP is used - 8.8.8.8)
     * Then: start of the second member fails.
     * </pre>
     */
    @Test
    public void testSanForeign() throws IOException {
        File keystoreFile = copyResource(KEYSTORE_SAN_FOREIGN);
        Config config = createMemberConfig(keystoreFile);
        factory.newHazelcastInstance(config);
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(config);
    }

    /**
     * <pre>
     * When: 2 members share one keystore without usable SAN entry, but with valid CN entry (127.0.0.1)
     * Then: the members connects and form 2 node cluster
     * </pre>
     */
    @Test
    public void testCnLoopback() throws IOException {
        File keystoreFile = copyResource(KEYSTORE_CN_LOOPBACK);
        Config config = createMemberConfig(keystoreFile);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
    }

    /**
     * <pre>
     * When: 2 members share one keystore which doens't have any entry from which hostname could be loaded (SAN or CN in subject name)
     * Then: start of the second member fails
     * </pre>
     */
    @Test
    public void testNoHostname() throws IOException {
        File keystoreFile = copyResource(KEYSTORE_NO_HOSTNAME);
        Config config = createMemberConfig(keystoreFile);
        factory.newHazelcastInstance(config);
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(config);
    }

    /**
     * This is a tricky one!
     *
     * <pre>
     * When: 2 members use different keystores, the certificate for first member has correct SAN the certificate for second one
     *   has no valid hostname entry
     * Then: cluster is formed
     * </pre>
     */
    @Test
    public void testSanValidInvalid() throws IOException {
        factory.newHazelcastInstance(createMemberConfig(copyResource(KEYSTORE_SAN_LOOPBACK)));
        factory.newHazelcastInstance(createMemberConfig(copyResource(KEYSTORE_NO_HOSTNAME)));
    }

    /**
     * This is a tricky one!
     *
     * <pre>
     * When: 2 members use different keystores, the certificate for first member has no valid hostname entry and the certificate for second one
     *   has valid SAN
     * Then: start of the second member fails
     * </pre>
     */
    @Test
    public void testSanInvalidValid() throws IOException {
        factory.newHazelcastInstance(createMemberConfig(copyResource(KEYSTORE_NO_HOSTNAME)));
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(createMemberConfig(copyResource(KEYSTORE_SAN_LOOPBACK)));
    }

    /**
     * <pre>
     * When: client tries to connect to a member which has a SAN entry which don't match the host
     * Then: connection fails
     * </pre>
     */
    @Test
    public void testClientSanForeign() throws IOException {
        File ksFile = copyResource(KEYSTORE_SAN_FOREIGN);
        factory.newHazelcastInstance(createMemberConfig(ksFile));
        expected.expect(IllegalStateException.class);
        factory.newHazelcastClient(createClientConfig(ksFile));
    }

    /**
     * <pre>
     * When: client tries to connect to a member which doesn't have a checked SAN entry, but has valid hostname in the CN field
     * Then: connection succeeds
     * </pre>
     */
    @Test
    public void testClientCnLoopback() throws IOException {
        File ksFile = copyResource(KEYSTORE_CN_LOOPBACK);
        factory.newHazelcastInstance(createMemberConfig(ksFile));
        factory.newHazelcastClient(createClientConfig(ksFile));
    }

    private Config createMemberConfig(File keyStoreFile) {
        Config config = new Config()
            .setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), CLUSTER_JOIN_MAX_SECONDS);
        config.getNetworkConfig()
            .setSSLConfig(createSSLConfig(keyStoreFile))
            .getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(CONNECTION_TIMEOUT_SECONDS);
        return config;
    }

    private ClientConfig createClientConfig(File keyStoreFile) {
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(0);
        SSLConfig sslConfig = createSSLConfig(mutualAuthentication ? keyStoreFile : null);
        config.getNetworkConfig().setSSLConfig(sslConfig);
        return config;
    }

    private SSLConfig createSSLConfig(File keyStoreFile) {
        SSLConfig sslConfig = new SSLConfig()
            .setEnabled(true)
            .setFactoryClassName(openSsl ? OpenSSLEngineFactory.class.getName() : BasicSSLContextFactory.class.getName())
            .setProperty("validateIdentity", "true")
            .setProperty("trustStore", trustStoreFile.getAbsolutePath())
            .setProperty("trustStorePassword", "123456")
            .setProperty("trustStoreType", "PKCS12");
        if (keyStoreFile != null) {
            sslConfig.setProperty("keyStore", keyStoreFile.getAbsolutePath())
                .setProperty("keyStorePassword", "123456")
                .setProperty("keyStoreType", "PKCS12");
        }
        if (mutualAuthentication) {
            sslConfig.setProperty("mutualAuthentication", "REQUIRED");
        }
        return sslConfig;
    }

    /**
     * Copies a resource file from the current package to a temporary folder.
     */
    private File copyResource(String resourceName) throws IOException {
        return copyTestResource(getClass(), tempFolder.getRoot(), resourceName);
    }

    /**
     * Throws {@link AssumptionViolatedException} if the "localhost" hostname doesn't resolve to 127.0.0.1.
     */
    public static void assumeLocalhostResolvesTo_127_0_0_1() {
        boolean resolvedToLoopback = false;
        try {
            InetAddress ia = InetAddress.getByName("localhost");
            resolvedToLoopback = "127.0.0.1".equals(ia.getHostAddress());
        } catch (UnknownHostException e) {
            // OK
        }
        assumeTrue("The localhost doesn't resolve to 127.0.0.1. Skipping the test.", resolvedToLoopback);
    }
}
