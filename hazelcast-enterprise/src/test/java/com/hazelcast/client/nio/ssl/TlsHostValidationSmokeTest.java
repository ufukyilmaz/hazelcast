package com.hazelcast.client.nio.ssl;

import static com.hazelcast.TestEnvironmentUtil.copyTestResource;
import static com.hazelcast.TestEnvironmentUtil.isOpenSslSupported;
import static java.util.Arrays.asList;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.OpenSSLEngineFactory;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Quick smoke tests for TLS host identity validation flag.
 * <p>
 * Given: Pregenerated key material is copied to a temporary folder. The default host validation is enabled for both members and
 * clients.
 * </p>
 *
 * @see TlsHostIdentityValidationTest
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({ QuickTest.class })
public class TlsHostValidationSmokeTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private static final String KEYSTORE_SAN_LOOPBACK = "tls-host-loopback-san.p12";
    private static final String KEYSTORE_NO_HOSTNAME = "tls-host-no-entry.p12";
    private static final String TRUSTSTORE = "truststore.p12";

    private static final int CONNECTION_TIMEOUT_SECONDS = 5;
    private static final String CLUSTER_JOIN_MAX_SECONDS = "5";

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @Parameter
    public boolean openSsl;

    @Parameters(name = "openSsl:{0}")
    public static Collection<Boolean> parameters() {
        return asList(false, true);
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
     * When: client tries to connect to a member which has a valid SAN entry (loopback)
     * Then: connection succeeds
     * </pre>
     */
    @Test
    public void testClientSanLoopback() throws IOException {
        File ksFile = copyResource(KEYSTORE_SAN_LOOPBACK);
        factory.newHazelcastInstance(createMemberConfig(ksFile));
        factory.newHazelcastClient(createClientConfig(ksFile));
    }

    /**
     * <pre>
     * When: client tries to connect to a member which doesn't have neither a checked SAN entry nor a CN field
     * Then: connection fails
     * </pre>
     */
    @Test
    public void testClientNoHostname() throws IOException {
        File ksFile = copyResource(KEYSTORE_NO_HOSTNAME);
        factory.newHazelcastInstance(createMemberConfig(ksFile));
        expected.expect(IllegalStateException.class);
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
        SSLConfig sslConfig = createSSLConfig(null);
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
        return sslConfig;
    }

    /**
     * Copies a resource file from the current package to a temporary folder.
     */
    private File copyResource(String resourceName) throws IOException {
        return copyTestResource(getClass(), tempFolder.getRoot(), resourceName);
    }
}
