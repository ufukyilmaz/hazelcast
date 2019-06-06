package com.hazelcast.client.bluegreen;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.ClientSelectors;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.IdentifiedDataSerializableFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.nio.Address;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class DisconnectViaCommandTest extends ClientTestSupport {

    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    private Member toMember(HazelcastInstance instance1) {
        return (Member) instance1.getLocalEndpoint();
    }

    @Test
    public void blacklistViaCommand() {
        Config config1 = new Config();
        config1.getGroupConfig().setName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.getGroupConfig().setName("dev2");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        Member member2 = (Member) instance2.getLocalEndpoint();
        Address address2 = member2.getAddress();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER.equals(event.getState())) {
                    countDownLatch.countDown();
                }
            }
        });
        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);

        getClientEngineImpl(instance1).applySelector(ClientSelectors.none());

        assertOpenEventually(countDownLatch);

        members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member2);
    }

    @Test
    public void blacklistViaCommand_checkInitialMembershipListeners() throws InterruptedException {
        Config config1 = new Config();
        config1.getGroupConfig().setName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.getGroupConfig().setName("dev2");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        Member member2 = (Member) instance2.getLocalEndpoint();
        Address address2 = member2.getAddress();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        final LinkedBlockingQueue<InitialMembershipEvent> events = new LinkedBlockingQueue<InitialMembershipEvent>();
        final AtomicInteger otherEvents = new AtomicInteger();
        ListenerConfig listenerConfig = new ListenerConfig(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                events.offer(event);
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                otherEvents.incrementAndGet();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                otherEvents.incrementAndGet();
            }

            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
                otherEvents.incrementAndGet();
            }
        });
        clientConfig.addListenerConfig(listenerConfig);
        clientConfig2.addListenerConfig(listenerConfig);

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        Set<Member> members = events.take().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);

        getClientEngineImpl(instance1).applySelector(ClientSelectors.none());

        members = events.take().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member2);
        assertEquals(0, otherEvents.get());
    }

    @Test
    public void blacklistViaCommand_differentPartitionCount_clientShouldShutdown() {
        Config config1 = new Config();
        config1.getGroupConfig().setName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setProperty(GroupProperty.PARTITION_COUNT.getName(), "2");
        config2.getGroupConfig().setName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.getGroupConfig().setName("dev2");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        Member member2 = (Member) instance2.getLocalEndpoint();
        Address address2 = member2.getAddress();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ListenerConfig listenerConfig = new ListenerConfig(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.SHUTDOWN.equals(event.getState())) {
                    countDownLatch.countDown();
                }
            }
        });
        clientConfig.addListenerConfig(listenerConfig);
        clientConfig2.addListenerConfig(listenerConfig);

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2).setTryCount(1);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);

        getClientEngineImpl(instance1).applySelector(ClientSelectors.none());

        assertOpenEventually(countDownLatch);
    }

    @Test
    public void blacklistViaCommand_listenerBehaviour() {
        Config config1 = new Config();
        config1.getGroupConfig().setName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.getGroupConfig().setName("dev2");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        Member member2 = (Member) instance2.getLocalEndpoint();
        Address address2 = member2.getAddress();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER.equals(event.getState())) {
                    countDownLatch.countDown();
                }
            }
        });
        final ITopic<Object> clientTopic = client.getTopic("map");

        final CountDownLatch topicMessageLatch = new CountDownLatch(1);
        clientTopic.addMessageListener(new MessageListener<Object>() {
            @Override
            public void onMessage(Message<Object> message) {
                topicMessageLatch.countDown();
            }
        });

        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);

        getClientEngineImpl(instance1).applySelector(ClientSelectors.none());

        assertOpenEventually(countDownLatch);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                clientTopic.publish("message");
                assertOpenEventually(topicMessageLatch, 5);
            }
        });
    }

    @Test
    public void blacklistViaCommand_nearCacheCleanup() {
        Config config1 = new Config();
        config1.getGroupConfig().setName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.getGroupConfig().setName("dev2");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        Member member2 = (Member) instance2.getLocalEndpoint();
        Address address2 = member2.getAddress();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setName("map");


        clientConfig.addNearCacheConfig(nearCacheConfig);
        clientConfig2.addNearCacheConfig(nearCacheConfig);


        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER.equals(event.getState())) {
                    countDownLatch.countDown();
                }
            }
        });

        IMap<Object, Object> map = client.getMap("map");
        map.put(1, 1);
        map.get(1);

        NearCache nearCache = getHazelcastClientInstanceImpl(client).getNearCacheManager().getNearCache("map");
        assertEquals(1, nearCache.size());

        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);

        getClientEngineImpl(instance1).applySelector(ClientSelectors.none());

        assertOpenEventually(countDownLatch);

        assertEquals(0, nearCache.size());

        members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member2);
    }

    @Test
    public void blacklistViaCommand_queryCacheBehaviour() {
        Config config1 = new Config();
        config1.getGroupConfig().setName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.getGroupConfig().setName("dev2");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        Member member2 = (Member) instance2.getLocalEndpoint();
        Address address2 = member2.getAddress();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER.equals(event.getState())) {
                    countDownLatch.countDown();
                }
            }
        });

        IMap<Object, Object> map = client.getMap("map");
        map.put(1, 1);

        QueryCache<Object, Object> queryCache = map.getQueryCache("map", Predicates.alwaysTrue(), true);
        assertEquals(1, queryCache.size());

        final CountDownLatch entryAddedLatch = new CountDownLatch(1);
        queryCache.addEntryListener(new EntryAddedListener<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                entryAddedLatch.countDown();
            }
        }, true);

        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);

        getClientEngineImpl(instance1).applySelector(ClientSelectors.none());

        assertOpenEventually(countDownLatch);
        assertEquals(0, queryCache.size());

        map.put(2, 2);
        assertOpenEventually(entryAddedLatch);

        assertEquals(1, queryCache.size());

        members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member2);
    }

    @Test(expected = TargetDisconnectedException.class)
    public void blacklistViaCommand_waitingOperationsGetsException() throws Throwable {
        Config config1 = new Config();
        config1.getGroupConfig().setName("dev1");
        addDataSerializableFactory(config1.getSerializationConfig());
        HazelcastInstance c1_instance1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance c1_instance2 = Hazelcast.newHazelcastInstance(config1);

        assertClusterSizeEventually(2, c1_instance1, c1_instance2);
        Config config2 = new Config();
        addDataSerializableFactory(config2.getSerializationConfig());
        config2.getGroupConfig().setName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);
        ClientConfig clientConfig = new ClientConfig();
        addDataSerializableFactory(clientConfig.getSerializationConfig());
        clientConfig.getGroupConfig().setName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member c1_member1 = toMember(c1_instance1);
        Address address1 = c1_member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.getGroupConfig().setName("dev2");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        addDataSerializableFactory(clientConfig2.getSerializationConfig());
        Member c2_member2 = (Member) instance2.getLocalEndpoint();
        Address address2 = c2_member2.getAddress();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        Member c1_member2 = toMember(c1_instance2);

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER.equals(event.getState())) {
                    countDownLatch.countDown();
                }
            }
        });

        Set<Member> members = client.getCluster().getMembers();
        assertEquals(2, members.size());
        assertContains(members, c1_member1);
        assertContains(members, c1_member2);

        IExecutorService executorService = client.getExecutorService("exec");
        ICountDownLatch callableStartedLatch = client.getCountDownLatch("callableStartedLatch");
        callableStartedLatch.trySetCount(1);
        IdentifiedDataSerializableFactory.CallableSignalsRunAndSleep callable =
                new IdentifiedDataSerializableFactory.CallableSignalsRunAndSleep("callableStartedLatch");
        Future<Boolean> future = executorService.submitToMember(callable, c1_member2);
        assertOpenEventually(callableStartedLatch);

        getClientEngineImpl(c1_instance1).applySelector(ClientSelectors.none());
        getClientEngineImpl(c1_instance2).applySelector(ClientSelectors.none());

        assertOpenEventually(countDownLatch);

        try {
            future.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    private void addDataSerializableFactory(SerializationConfig serializationConfig) {
        serializationConfig.addDataSerializableFactory(IdentifiedDataSerializableFactory.FACTORY_ID,
                new IdentifiedDataSerializableFactory());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void clientWontStartWithIllegalConfig() {
        Config config1 = new Config();
        config1.getGroupConfig().setName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.getGroupConfig().setName("dev2");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        Member member2 = (Member) instance2.getLocalEndpoint();
        Address address2 = member2.getAddress();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        //illegal change on the config that will cause the exception
        clientConfig2.setProperty("newProperty", "newValue");

        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

    }

    @Test
    public void blacklistViaCommand_connectBackToSingleClusterAfterRemovingBlacklist() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));


        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig);
        final HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch changedClusterLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED.equals(event.getState())) {
                    disconnectedLatch.countDown();
                }

                if (LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER.equals(event.getState())) {
                    changedClusterLatch.countDown();
                }
            }
        });
        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);

        getClientEngineImpl(instance).applySelector(ClientSelectors.none());

        assertOpenEventually(disconnectedLatch);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, client.getCluster().getMembers().size());
            }
        });

        getClientEngineImpl(instance).applySelector(ClientSelectors.any());

        assertOpenEventually(changedClusterLatch);

        members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);
    }

    @Test
    public void testBlue_toGreen_toBackToBlue() throws InterruptedException {
        Config config1 = new Config();
        config1.getGroupConfig().setName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1:5701").setEnabled(true);
        HazelcastInstance cluster1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1:5702").setEnabled(true);
        HazelcastInstance cluster2 = Hazelcast.newHazelcastInstance(config2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress("127.0.0.1:5701");

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.getGroupConfig().setName("dev2");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        networkConfig2.addAddress("127.0.0.1:5702");

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2).setTryCount(1);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final CountDownLatch firstClusterChange = new CountDownLatch(1);
        final CountDownLatch secondCLusterChange = new CountDownLatch(1);
        final AtomicInteger atomicInteger = new AtomicInteger();
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER.equals(event.getState())) {
                    int changedCount = atomicInteger.incrementAndGet();
                    if (changedCount == 1) {
                        firstClusterChange.countDown();
                    } else if (changedCount == 2) {
                        secondCLusterChange.countDown();
                    }
                }
            }
        });

        getClientEngineImpl(cluster1).applySelector(ClientSelectors.none());
        assertOpenEventually(firstClusterChange);

        getClientEngineImpl(cluster1).applySelector(ClientSelectors.any());
        getClientEngineImpl(cluster2).applySelector(ClientSelectors.none());
        assertOpenEventually(secondCLusterChange);

        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, cluster1.getCluster().getLocalMember());
    }
}

