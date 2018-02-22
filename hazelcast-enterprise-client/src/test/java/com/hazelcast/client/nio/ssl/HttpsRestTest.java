package com.hazelcast.client.nio.ssl;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.net.ssl.TrustManagerFactory;
import java.util.Properties;

import static com.hazelcast.client.nio.ssl.ClientAuthenticationTest.makeFile;
import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.loadTrustManagerFactory;
import static com.hazelcast.spi.properties.GroupProperty.REST_ENABLED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HttpsRestTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private HTTPCommunicator communicator;

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setup() throws Exception {
        Properties serverProps = new Properties();
        serverProps.setProperty("javax.net.ssl.keyStore", makeFile("server1_knows_noone/server1.keystore"));
        serverProps.setProperty("javax.net.ssl.keyStorePassword", "password");
        serverProps.setProperty("javax.net.ssl.trustStorePassword", "password");
        serverProps.setProperty("javax.net.ssl.protocol", "TLS");

        Config config = new Config()
                .setProperty(REST_ENABLED.getName(), "true")
                .setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig()
                .setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1")
                .setConnectionTimeoutSeconds(3000);
        networkConfig.setSSLConfig(new SSLConfig()
                .setEnabled(true)
                .setProperties(serverProps));
        instance = Hazelcast.newHazelcastInstance(config);

        TrustManagerFactory trustManagerFactory = loadTrustManagerFactory(
                "password",
                makeFile("anonymous_client1_knows_server1/client1.truststore"),
                TrustManagerFactory.getDefaultAlgorithm());

        communicator = new HTTPCommunicator(instance)
                .setClientTrustManagers(trustManagerFactory)
                .setTlsProtocol(serverProps.getProperty("javax.net.ssl.protocol"));
    }

    @Test
    public void test() throws Exception {
        String name = randomMapName();

        String key = "key";
        String value = "value";

        assertEquals(HTTP_OK, communicator.mapPut(name, key, value));
        assertEquals(value, communicator.mapGet(name, key));
        assertTrue(instance.getMap(name).containsKey(key));
    }
}
