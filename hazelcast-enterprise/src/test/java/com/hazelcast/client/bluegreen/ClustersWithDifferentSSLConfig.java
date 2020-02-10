package com.hazelcast.client.bluegreen;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.ClientSelectors;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getOrCreateTempFile;
import static com.hazelcast.test.Accessors.getClientEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClustersWithDifferentSSLConfig extends ClientTestSupport {

    private final String keyStore1 = "com/hazelcast/nio/ssl/hazelcast.keystore";
    private final String trustStore1 = "com/hazelcast/nio/ssl/hazelcast.truststore";
    private final String keyStore2 = "com/hazelcast/nio/ssl/hazelcast2.keystore";
    private final String trustStore2 = "com/hazelcast/nio/ssl/hazelcast2.truststore";

    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    private Member toMember(HazelcastInstance instance1) {
        return (Member) instance1.getLocalEndpoint();
    }

    private SSLConfig createMemberSSlConfig(String keyStore, String trustStore) {
        Properties serverProps = new Properties();
        serverProps.setProperty("javax.net.ssl.keyStore", getOrCreateTempFile(keyStore));
        serverProps.setProperty("javax.net.ssl.trustStore", getOrCreateTempFile(trustStore));
        serverProps.setProperty("javax.net.ssl.keyStorePassword", "123456");
        serverProps.setProperty("javax.net.ssl.trustStorePassword", "123456");
        serverProps.setProperty("javax.net.ssl.protocol", "TLS");
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperties(serverProps);
        sslConfig.setEnabled(true);
        return sslConfig;
    }

    private SSLConfig createClientSSlConfig(String trustStore) {
        Properties properties = new Properties();
        properties.setProperty("javax.net.ssl.trustStore", getOrCreateTempFile(trustStore));
        properties.setProperty("javax.net.ssl.trustStorePassword", "123456");
        properties.setProperty("javax.net.ssl.protocol", "TLS");
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperties(properties);
        sslConfig.setEnabled(true);
        return sslConfig;
    }

    @Test
    public void test_betweenTwoSSLEnabled() {
        Config config1 = new Config();
        config1.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        config1.setClusterName("dev1");
        config1.getNetworkConfig().setSSLConfig(createMemberSSlConfig(keyStore1, trustStore1));
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        config2.setClusterName("dev2");
        config2.getNetworkConfig().setSSLConfig(createMemberSSlConfig(keyStore2, trustStore2));
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));
        networkConfig.setSSLConfig(createClientSSlConfig(trustStore1));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.setClusterName("dev2");
        Member member2 = toMember(instance2);
        Address address2 = member2.getAddress();
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));
        networkConfig2.setSSLConfig(createClientSSlConfig(trustStore2));

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
    public void test_migrationToSSLEnabled() {
        Config config1 = new Config();
        config1.setClusterName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        config2.setClusterName("dev2");
        config2.getNetworkConfig().setSSLConfig(createMemberSSlConfig(keyStore2, trustStore2));
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.setClusterName("dev2");
        Member member2 = toMember(instance2);
        Address address2 = member2.getAddress();
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));
        networkConfig2.setSSLConfig(createClientSSlConfig(trustStore2));

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
    public void test_migrationFromSSLEnabled() {
        Config config1 = new Config();
        config1.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        config1.setClusterName("dev1");
        config1.getNetworkConfig().setSSLConfig(createMemberSSlConfig(keyStore1, trustStore1));
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setClusterName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));
        networkConfig.setSSLConfig(createClientSSlConfig(trustStore1));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.setClusterName("dev2");
        Member member2 = toMember(instance2);
        Address address2 = member2.getAddress();
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
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

}
