package com.hazelcast.client.enterprise;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.tcp.SocketInterceptorTest.MySocketInterceptor;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientSocketInterceptorTest {

    @Before
    @After
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 120000)
    public void testSuccessfulSocketInterceptor() {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);
        SocketInterceptorConfig socketInterceptorConfig = new SocketInterceptorConfig();
        MySocketInterceptor mySocketInterceptor = new MySocketInterceptor(true);
        socketInterceptorConfig.setImplementation(mySocketInterceptor).setEnabled(true);
        config.getNetworkConfig().setSocketInterceptorConfig(socketInterceptorConfig);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(2, h1, h2);

        ClientConfig clientConfig = new ClientConfig();
        MySocketInterceptor myClientSocketInterceptor = new MySocketInterceptor(true);
        clientConfig.getNetworkConfig().setSocketInterceptorConfig(new SocketInterceptorConfig().setEnabled(true)
                .setImplementation(myClientSocketInterceptor));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        assertClusterSize(2, client);

        assertBetween("Accept call count should be 2 or 3", 2, 3, mySocketInterceptor.getAcceptCallCount());
        assertEquals(1, mySocketInterceptor.getConnectCallCount());
        assertEquals(0, mySocketInterceptor.getAcceptFailureCount());
        assertEquals(0, mySocketInterceptor.getConnectFailureCount());

        assertBetween("Client connect call count should be 1 or 2", 1, 2, myClientSocketInterceptor.getConnectCallCount());
        assertEquals(0, myClientSocketInterceptor.getAcceptCallCount());
        assertEquals(0, myClientSocketInterceptor.getAcceptFailureCount());
        assertEquals(0, myClientSocketInterceptor.getConnectFailureCount());
    }

    static void assertBetween(String message, int begin, int end, int value) {
        assertTrue(message, value >= begin);
        assertTrue(message, value <= end);
    }


    @Test(timeout = 120000)
    public void testFailingSocketInterceptor() {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);
        SocketInterceptorConfig socketInterceptorConfig = new SocketInterceptorConfig();
        MySocketInterceptor mySocketInterceptor = new MySocketInterceptor(true);
        socketInterceptorConfig.setImplementation(mySocketInterceptor).setEnabled(true);
        config.getNetworkConfig().setSocketInterceptorConfig(socketInterceptorConfig);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(2, h1, h2);

        ClientConfig clientConfig = new ClientConfig();
        MySocketInterceptor myClientSocketInterceptor = new MySocketInterceptor(false);
        clientConfig.getNetworkConfig().setSocketInterceptorConfig(new SocketInterceptorConfig().setEnabled(true)
                .setImplementation(myClientSocketInterceptor));

        try {
            HazelcastClient.newHazelcastClient(clientConfig);
            fail("Client should not be able to connect!");
        } catch (IllegalStateException e) {
            assertTrue(mySocketInterceptor.getAcceptFailureCount() > 0);
            assertTrue(myClientSocketInterceptor.getConnectFailureCount() > 0);
        }

    }
}
