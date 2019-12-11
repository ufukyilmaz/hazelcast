package com.hazelcast.client.bluegreen;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class FailoverTest extends ClientTestSupport {

    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testFailover_readOnlyOperationsRetried() {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(createConfig("dev1"));
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(createConfig("dev2"));
        ClientConfig clientConfig = createClientConfig(instance1.getCluster().getLocalMember(), "dev1");
        ClientConfig clientConfig2 = createClientConfig(instance2.getCluster().getLocalMember(), "dev2");

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> {
            if (LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER.equals(event.getState())) {
                countDownLatch.countDown();
            }
        });
        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, instance1.getCluster().getLocalMember());

        final IMap<Object, Object> map = client.getMap("test");

        final AtomicReference<Throwable> reference = new AtomicReference<>();
        new Thread(() -> {
            try {
                while (countDownLatch.getCount() != 0) {
                    map.get(1);
                }
            } catch (Exception e) {
                reference.set(e);
            }
        }).start();

        instance1.shutdown();

        assertOpenEventually(countDownLatch);
        assertNull(reference.get());

        members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, instance2.getCluster().getLocalMember());
    }

    private Config createConfig(String dev1) {
        Config config1 = new Config();
        config1.setClusterName(dev1);
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        return config1;
    }

    private ClientConfig createClientConfig(Member member, String clusterName) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(1000);
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Address address = member.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address.getHost() + ":" + address.getPort()));
        return clientConfig;
    }

    @Test
    public void testOperationsGetOfflineException_clientInReconnectAsyncMode_whenSearchingForNewCluster() {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(createConfig("dev"));

        ClientConfig clientConfig = createClientConfig(instance1.getCluster().getLocalMember(), "dev");
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);


        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig);
        clientFailoverConfig.setTryCount(Integer.MAX_VALUE);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final IMap<Object, Object> map = client.getMap("test");

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Class> reference = new AtomicReference<Class>();
        new Thread(() -> {
            try {
                while (true) {
                    map.get(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
                reference.set(e.getClass());
                countDownLatch.countDown();
            }
        }).start();

        instance1.shutdown();

        assertOpenEventually(countDownLatch);
        assertEquals(HazelcastClientOfflineException.class, reference.get());

    }


    @Test
    public void testFailover_differentPartitionCount_clientShouldClose() {
        Config config1 = createConfig("dev1");
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = createConfig("dev2");
        config2.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");

        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);
        ClientConfig clientConfig = createClientConfig(instance1.getCluster().getLocalMember(), "dev1");
        ClientConfig clientConfig2 = createClientConfig(instance2.getCluster().getLocalMember(), "dev2");

        CountDownLatch countDownLatch = new CountDownLatch(1);
        ListenerConfig listenerConfig = new ListenerConfig((LifecycleListener) event -> {
            if (LifecycleEvent.LifecycleState.SHUTDOWN.equals(event.getState())) {
                countDownLatch.countDown();
            }
        });
        clientConfig.addListenerConfig(listenerConfig);
        clientConfig2.addListenerConfig(listenerConfig);

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2).setTryCount(1);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);


        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, instance1.getCluster().getLocalMember());

        instance1.shutdown();

        assertOpenEventually(countDownLatch);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testFailover_illegalConfigChangeOnFailoverClientConfigs() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBackupAckToClientEnabled(true);
        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.setBackupAckToClientEnabled(false);

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);
    }
}

