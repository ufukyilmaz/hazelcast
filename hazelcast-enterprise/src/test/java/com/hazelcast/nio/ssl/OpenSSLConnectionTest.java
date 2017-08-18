package com.hazelcast.nio.ssl;

import com.hazelcast.IbmUtil;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import io.netty.handler.ssl.OpenSsl;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.nio.ssl.OpenSSLEngineFactory.JAVA_NET_SSL_PREFIX;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class OpenSSLConnectionTest {

    @BeforeClass
    public static void checkOpenSsl() {
        assumeTrue(OpenSsl.isAvailable());
        assumeFalse(IbmUtil.ibmJvm());
    }

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void test() throws IOException {
        Config config = newConfig();

        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, h1.getCluster().getMembers().size());
                assertEquals(2, h2.getCluster().getMembers().size());
            }
        });
    }

    @Test(expected = HazelcastException.class)
    public void testUnknownCipherSuite() throws IOException {
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty(JAVA_NET_SSL_PREFIX + "ciphersuites","unknown");

        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testTLSProtocol() throws IOException {
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty(JAVA_NET_SSL_PREFIX + "protocol","TLS");

        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testSSLProtocol() throws IOException {
        Config config = newConfig();
        config.getNetworkConfig().getSSLConfig().setProperty(JAVA_NET_SSL_PREFIX + "protocol","SSL");

        Hazelcast.newHazelcastInstance(config);
    }

    private Config newConfig() throws IOException {
        Config config = new Config();
        config.setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1")
                .setConnectionTimeoutSeconds(3000);

        Properties sslProperties = TestKeyStoreUtil.createSslProperties();
        sslProperties.setProperty(JAVA_NET_SSL_PREFIX + "openssl", "true");
        SSLConfig sslConfig = new SSLConfig()
                .setEnabled(true)
                .setFactoryImplementation(new OpenSSLEngineFactory())
                .setProperties(sslProperties);
        config.getNetworkConfig().setSSLConfig(sslConfig);
        return config;
    }
}
