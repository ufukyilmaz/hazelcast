/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.bluegreen;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

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
    public void testFailover() {
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
        Member member1 = (Member) instance1.getLocalEndpoint();
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

        instance1.shutdown();

        assertOpenEventually(countDownLatch);

        members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member2);
    }

    @Test
    public void testFailover_differentPartitionCount_clientShouldClose() {
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
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = (Member) instance1.getLocalEndpoint();
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

        instance1.shutdown();

        assertOpenEventually(countDownLatch);
    }
}

