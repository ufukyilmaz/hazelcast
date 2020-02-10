package com.hazelcast.client.bluegreen;

import com.google.common.base.Charsets;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.ClientSelectors;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.Accessors.getClientEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClustersWithDifferentSocketInterceptorsTest extends ClientTestSupport {

    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    private Member toMember(HazelcastInstance instance1) {
        return (Member) instance1.getLocalEndpoint();
    }

    public static class CustomSocketInterceptor implements MemberSocketInterceptor {

        private String secret;

        @Override
        public void init(Properties properties) {
            secret = properties.getProperty("secret");
        }

        @Override
        public void onAccept(Socket acceptedSocket) throws IOException {
            InputStream inputStream = acceptedSocket.getInputStream();
            int length = inputStream.read();
            byte[] bytes = new byte[length];
            int read = inputStream.read(bytes);
            if (read != length) {
                throw new IOException("insufficient data");
            }
            String fromClient = new String(bytes, Charsets.UTF_8);
            if (!secret.equals(fromClient)) {
                throw new IOException("Given secret is wrong");
            }

        }

        @Override
        public void onConnect(Socket connectedSocket) throws IOException {
            byte[] bytes = secret.getBytes(Charsets.UTF_8);
            OutputStream outputStream = connectedSocket.getOutputStream();
            outputStream.write(bytes.length);
            outputStream.write(bytes);
        }
    }

    @Test
    public void test_betweenTwoClustersWithSocketInterceptorEnabled() {
        Config config1 = new Config();
        config1.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        SocketInterceptorConfig socketInterceptorConfig1 = new SocketInterceptorConfig();
        socketInterceptorConfig1.setEnabled(true);
        socketInterceptorConfig1.setProperty("secret", "cluster1Secret");
        socketInterceptorConfig1.setClassName(CustomSocketInterceptor.class.getName());
        config1.setClusterName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config1.getNetworkConfig().setSocketInterceptorConfig(socketInterceptorConfig1);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        SocketInterceptorConfig socketInterceptorConfig2 = new SocketInterceptorConfig();
        socketInterceptorConfig2.setEnabled(true);
        socketInterceptorConfig2.setProperty("secret", "cluster2Secret");
        socketInterceptorConfig2.setClassName(CustomSocketInterceptor.class.getName());
        config2.getNetworkConfig().setSocketInterceptorConfig(socketInterceptorConfig2);
        config2.setClusterName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        SocketInterceptorConfig clientSocketInterceptorConfig1 = new SocketInterceptorConfig();
        clientSocketInterceptorConfig1.setEnabled(true);
        clientSocketInterceptorConfig1.setProperty("secret", "cluster1Secret");
        clientSocketInterceptorConfig1.setClassName(CustomSocketInterceptor.class.getName());
        networkConfig.setSocketInterceptorConfig(clientSocketInterceptorConfig1);

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.setClusterName("dev2");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        Member member2 = toMember(instance2);
        Address address2 = member2.getAddress();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        SocketInterceptorConfig clientSocketInterceptorConfig2 = new SocketInterceptorConfig();
        clientSocketInterceptorConfig2.setEnabled(true);
        clientSocketInterceptorConfig2.setProperty("secret", "cluster2Secret");
        clientSocketInterceptorConfig2.setClassName(CustomSocketInterceptor.class.getName());
        clientConfig2.getNetworkConfig().setSocketInterceptorConfig(clientSocketInterceptorConfig2);

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
    public void test_migrationToSocketInterceptorEnabled() {
        Config config1 = new Config();
        config1.setClusterName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        SocketInterceptorConfig socketInterceptorConfig2 = new SocketInterceptorConfig();
        socketInterceptorConfig2.setEnabled(true);
        socketInterceptorConfig2.setProperty("secret", "cluster2Secret");
        socketInterceptorConfig2.setClassName(CustomSocketInterceptor.class.getName());
        config2.getNetworkConfig().setSocketInterceptorConfig(socketInterceptorConfig2);
        config2.setClusterName("dev2");
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
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        Member member2 = toMember(instance2);
        Address address2 = member2.getAddress();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        SocketInterceptorConfig clientSocketInterceptorConfig2 = new SocketInterceptorConfig();
        clientSocketInterceptorConfig2.setEnabled(true);
        clientSocketInterceptorConfig2.setProperty("secret", "cluster2Secret");
        clientSocketInterceptorConfig2.setClassName(CustomSocketInterceptor.class.getName());
        clientConfig2.getNetworkConfig().setSocketInterceptorConfig(clientSocketInterceptorConfig2);

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
    public void test_migrationFromSocketInterceptorEnabled() {
        Config config1 = new Config();
        config1.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        SocketInterceptorConfig socketInterceptorConfig1 = new SocketInterceptorConfig();
        socketInterceptorConfig1.setEnabled(true);
        socketInterceptorConfig1.setProperty("secret", "cluster1Secret");
        socketInterceptorConfig1.setClassName(CustomSocketInterceptor.class.getName());
        config1.getNetworkConfig().setSocketInterceptorConfig(socketInterceptorConfig1);
        config1.setClusterName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setClusterName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        SocketInterceptorConfig clientSocketInterceptorConfig1 = new SocketInterceptorConfig();
        clientSocketInterceptorConfig1.setEnabled(true);
        clientSocketInterceptorConfig1.setProperty("secret", "cluster1Secret");
        clientSocketInterceptorConfig1.setClassName(CustomSocketInterceptor.class.getName());
        networkConfig.setSocketInterceptorConfig(clientSocketInterceptorConfig1);

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.setClusterName("dev2");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        Member member2 = toMember(instance2);
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

}
