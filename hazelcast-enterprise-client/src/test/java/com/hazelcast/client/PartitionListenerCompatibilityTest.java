/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;


import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Set;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class PartitionListenerCompatibilityTest extends HazelcastTestSupport {

    private CompatibilityTestHazelcastFactory factory = new CompatibilityTestHazelcastFactory();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void testPartitionListener_oldClient() {
        String oldestKnownVersion = "3.7";
        String currentVersion = CompatibilityTestHazelcastInstanceFactory.getCurrentVersion();

        HazelcastInstance instance1 = factory.newHazelcastInstance(currentVersion, new Config());
        HazelcastInstance instance2 = factory.newHazelcastInstance(currentVersion, new Config());
        HazelcastInstance client = factory.newHazelcastClient(oldestKnownVersion, new ClientConfig());

        testPartitionChangeGetsToClient(instance1, instance2, client);
    }


    @Test
    public void testPartitionListener_oldServer() {
        String oldestKnownVersion = "3.7";
        String currentVersion = CompatibilityTestHazelcastInstanceFactory.getCurrentVersion();

        HazelcastInstance instance1 = factory.newHazelcastInstance(oldestKnownVersion, new Config());
        HazelcastInstance instance2 = factory.newHazelcastInstance(oldestKnownVersion, new Config());
        HazelcastInstance client = factory.newHazelcastClient(currentVersion, new ClientConfig());

        testPartitionChangeGetsToClient(instance1, instance2, client);
    }

    @Test
    public void testPartitionListener() {
        String currentVersion = CompatibilityTestHazelcastInstanceFactory.getCurrentVersion();

        HazelcastInstance instance1 = factory.newHazelcastInstance(currentVersion, new Config());
        HazelcastInstance instance2 = factory.newHazelcastInstance(currentVersion, new Config());
        HazelcastInstance client = factory.newHazelcastClient(currentVersion, new ClientConfig());

        testPartitionChangeGetsToClient(instance1, instance2, client);
    }


    private void testPartitionChangeGetsToClient(final HazelcastInstance instance1, HazelcastInstance instance2,
                                                 final HazelcastInstance client) {
        warmUpPartitions(instance1, instance2, client);
        instancesHavePartition(instance1, instance2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Set<Partition> serverPartitions = instance1.getPartitionService().getPartitions();
                Set<Partition> clientPartitions = client.getPartitionService().getPartitions();
                assertPartitions(serverPartitions, clientPartitions);
            }
        });

        Member member2 = instance2.getCluster().getLocalMember();
        instance2.shutdown();
        memberDoesNotHavePartition(instance1, member2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Set<Partition> serverPartitions = instance1.getPartitionService().getPartitions();
                Set<Partition> clientPartitions = client.getPartitionService().getPartitions();
                assertPartitions(serverPartitions, clientPartitions);
            }
        });
    }

    public static void instancesHavePartition(HazelcastInstance... instances) {
        for (final HazelcastInstance instance : instances) {
            if (instance == null) {
                continue;
            }
            final PartitionService ps = instance.getPartitionService();
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    boolean havePartition = false;
                    for (Partition partition : ps.getPartitions()) {
                        havePartition = partition.getOwner().getUuid().equals(instance.getCluster().getLocalMember().getUuid());
                        if (havePartition) {
                            break;
                        }
                    }
                    assertTrue(havePartition);
                }
            });
        }
    }

    public static void memberDoesNotHavePartition(HazelcastInstance instance, final Member member) {
        final PartitionService ps = instance.getPartitionService();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (Partition partition : ps.getPartitions()) {
                    assertNotEquals(partition.getOwner().getUuid(), member.getUuid());
                }
            }
        });
    }

    private void assertPartitions(Set<Partition> partitions1, Set<Partition> partitions2) {
        HashMap<Integer, String> map = new HashMap<Integer, String>();
        for (Partition partition : partitions2) {
            Member owner = partition.getOwner();
            assertNotNull(owner);
            map.put(partition.getPartitionId(), owner.getUuid());
        }

        for (Partition partition : partitions1) {
            assertEquals(partition.getOwner().getUuid(), map.remove(partition.getPartitionId()));
        }

        assertEquals(0, map.size());
    }

}
