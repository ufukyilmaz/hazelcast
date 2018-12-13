package com.hazelcast.nio.ssl;

import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.TestEnvironmentUtil.assumeJdk8OrNewer;
import static com.hazelcast.TestEnvironmentUtil.assumeNoIbmJvm;
import static com.hazelcast.TestEnvironmentUtil.assumeThatOpenSslIsSupported;
import static com.hazelcast.TestEnvironmentUtil.copyTestResource;
import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.JAVA_NET_SSL_PREFIX;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class})
public class OpenSSLConnectionTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

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
    public void testEmptyTrust() {
        assumeNoIbmJvm();
        assumeJdk8OrNewer();
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().getProperties().remove("trustCertCollectionFile");
        // cover also the keyManagerFactory case (see OpenSSLConnectionKeyManagerFactoryTest)
        config.getNetworkConfig().getSSLConfig().getProperties().remove(JAVAX_NET_SSL_TRUST_STORE);

        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, h1, h2);
    }

    @Test(expected = HazelcastException.class)
    public void testUnknownCipherSuite() {
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty(JAVA_NET_SSL_PREFIX + "ciphersuites", "unknown");

        factory.newHazelcastInstance(config);
    }

    @Test
    public void testTLSProtocol() {
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty(JAVA_NET_SSL_PREFIX + "protocol", "TLS");

        factory.newHazelcastInstance(config);
    }

    @Test
    public void testSSLProtocol() {
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty(JAVA_NET_SSL_PREFIX + "protocol", "SSL");

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

        Config config = new Config().setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        config.getNetworkConfig().setSSLConfig(sslConfig);
        return config;
    }
}
