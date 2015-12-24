/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.enterprise;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.tcp.SocketInterceptorTest.MySocketInterceptor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientSocketInterceptorTest {

    @Before
    @After
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Ignore
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
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(2, h1.getCluster().getMembers().size());

        ClientConfig clientConfig = new ClientConfig();
        MySocketInterceptor myClientSocketInterceptor = new MySocketInterceptor(true);
        clientConfig.getNetworkConfig().setSocketInterceptorConfig(new SocketInterceptorConfig().setEnabled(true)
                .setImplementation(myClientSocketInterceptor));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        assertEquals(2, client.getCluster().getMembers().size());

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
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(2, h1.getCluster().getMembers().size());

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
