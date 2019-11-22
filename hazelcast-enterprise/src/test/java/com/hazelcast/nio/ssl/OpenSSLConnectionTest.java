package com.hazelcast.nio.ssl;

import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

import io.netty.handler.ssl.OpenSsl;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.TestEnvironmentUtil.assumeThatOpenSslIsSupported;
import static com.hazelcast.TestEnvironmentUtil.copyTestResource;
import static com.hazelcast.internal.nio.ssl.SSLEngineFactorySupport.JAVA_NET_SSL_PREFIX;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.hamcrest.CoreMatchers.containsString;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class})
public class OpenSSLConnectionTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @BeforeClass
    public static void checkOpenSsl() {
        assumeThatOpenSslIsSupported();
        killAllHazelcastInstances();
    }

    @AfterClass
    public static void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void test() {
        Config config = newConfig();

        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, h1, h2);
    }

    @Test
    public void testFipsMode_failsForBoringSSL() {
        Assume.assumeThat(OpenSsl.versionString(), containsString("BoringSSL"));
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty("fipsMode", "true");
        expectedException.expect(HazelcastException.class);
        factory.newHazelcastInstance(config);
    }

    @Test
    public void testUnknownCipherSuite() {
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty(JAVA_NET_SSL_PREFIX + "ciphersuites", "unknown");
        expectedException.expect(HazelcastException.class);
        factory.newHazelcastInstance(config);
    }

    @Test
    public void testTLSProtocol() {
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty(JAVA_NET_SSL_PREFIX + "protocol", "TLS");

        factory.newHazelcastInstance(config);
    }

    protected Config newConfig() {
        Properties sslProperties = new Properties();
        sslProperties.setProperty("keyFile",
                copyTestResource(getClass(), tempFolder.getRoot(), "privkey.pem").getAbsolutePath());
        sslProperties.setProperty("keyCertChainFile",
                copyTestResource(getClass(), tempFolder.getRoot(), "fullchain.pem").getAbsolutePath());
        sslProperties.setProperty("trustCertCollectionFile",
                copyTestResource(getClass(), tempFolder.getRoot(), "chain.pem").getAbsolutePath());
        SSLConfig sslConfig = new SSLConfig().setEnabled(true).setFactoryImplementation(new OpenSSLEngineFactory())
                .setProperties(sslProperties);

        Config config = smallInstanceConfig().setProperty(ClusterProperty.IO_THREAD_COUNT.getName(), "1");
        config.getNetworkConfig().setSSLConfig(sslConfig);
        return config;
    }
}
