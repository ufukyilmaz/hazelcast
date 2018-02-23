package com.hazelcast.nio.ssl;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.TestEnvironmentUtil.assumeThatOpenSslIsSupported;
import static com.hazelcast.nio.ssl.OpenSSLEngineFactory.JAVA_NET_SSL_PREFIX;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class OpenSSLConnectionTest {

    @BeforeClass
    public static void checkOpenSsl() {
        assumeThatOpenSslIsSupported();
    }

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void test() {
        Config config = newConfig();

        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(2, h1.getCluster().getMembers().size());
                assertEquals(2, h2.getCluster().getMembers().size());
            }
        });
    }

    @Test(expected = HazelcastException.class)
    public void testUnknownCipherSuite() {
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty(JAVA_NET_SSL_PREFIX + "ciphersuites", "unknown");

        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testTLSProtocol() {
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty(JAVA_NET_SSL_PREFIX + "protocol", "TLS");

        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testSSLProtocol() {
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty(JAVA_NET_SSL_PREFIX + "protocol", "SSL");

        Hazelcast.newHazelcastInstance(config);
    }

    private Config newConfig() {
        Properties sslProperties = TestKeyStoreUtil.createSslProperties();
        sslProperties.setProperty(JAVA_NET_SSL_PREFIX + "openssl", "true");
        SSLConfig sslConfig = new SSLConfig()
                .setEnabled(true)
                .setFactoryImplementation(new OpenSSLEngineFactory())
                .setProperties(sslProperties);

        Config config = new Config()
                .setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        NetworkConfig networkConfig = config.getNetworkConfig()
                .setSSLConfig(sslConfig);
        networkConfig.getJoin().getMulticastConfig()
                .setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1")
                .setConnectionTimeoutSeconds(3000);
        return config;
    }
}
