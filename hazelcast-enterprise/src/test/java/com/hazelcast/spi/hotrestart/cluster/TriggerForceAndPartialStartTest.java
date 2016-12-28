/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TriggerForceAndPartialStartTest extends AbstractHotRestartClusterStartTest {

    @Test
    public void test_triggerForceStart_onMaster() throws InterruptedException {
        test_triggerForceStart(false,true);
    }

    @Test
    public void test_triggerForceStart_onNonMaster() throws InterruptedException {
        test_triggerForceStart(false,false);
    }

    @Test
    public void test_triggerPartialStart_onMaster() throws InterruptedException {
        test_triggerForceStart(true,true);
    }

    @Test
    public void test_triggerPartialStart_onNonMaster() throws InterruptedException {
        test_triggerForceStart(true,false);
    }

    private void test_triggerForceStart(boolean partialStart, boolean onMaster) throws InterruptedException {
        int numberOfInstances = 3;
        HazelcastInstance[] instances = startNewInstances(numberOfInstances);
        assertInstancesJoined(numberOfInstances, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        warmUpPartitions(instances);

        Address[] addresses = Arrays.copyOf(getAddresses(instances), numberOfInstances - 1);
        shutdownCluster(instances);

        int expectedMemberCount = addresses.length;
        instances = restartInstances(addresses, createListenerMap(addresses, partialStart, onMaster, expectedMemberCount),
                HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE);
        assertInstancesJoined(expectedMemberCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        for (HazelcastInstance instance : instances) {
            checkStartResult(partialStart, instance);
        }
    }

    private void checkStartResult(boolean partialStart, HazelcastInstance instance) {
        InternalPartitionService partitionService = getNodeEngineImpl(instance).getPartitionService();
        InternalPartition partition = partitionService.getPartition(0, false);
        if (partialStart) {
            assertThat(partitionService.getPartitionStateVersion(), greaterThan(0));
            assertNotNull("Partition should have owner after partial start", partition.getOwnerOrNull());
        } else {
            assertEquals(0, partitionService.getPartitionStateVersion());
            assertNull("Partitions shouldn't have owner after force start", partition.getOwnerOrNull());
        }
    }

    @Override
    protected Config newConfig(String instanceName, ClusterHotRestartEventListener listener,
            HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {

        Config config = super.newConfig(instanceName, listener, clusterStartPolicy);
        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        // Tests don't depend on validation timeout, we explicitly trigger force & partial start.
        hotRestartPersistenceConfig.setValidationTimeoutSeconds(Integer.MAX_VALUE);
        return config;
    }

    private static void shutdownCluster(HazelcastInstance... instances) {
        assertThat(instances, not(emptyArray()));
        waitAllForSafeState(instances);
        instances[0].getCluster().shutdown();
    }

    private static Map<Address, ClusterHotRestartEventListener> createListenerMap(Address[] addresses,
            boolean partialStart, boolean onMaster, int expectedMemberCount) {

        Map<Address, ClusterHotRestartEventListener> listenerMap =
                new HashMap<Address, ClusterHotRestartEventListener>(addresses.length);
        for (Address address : addresses) {
            listenerMap.put(address, new TriggerForceStartListener(partialStart, onMaster, expectedMemberCount));
        }
        return listenerMap;
    }

    private static class TriggerForceStartListener extends ClusterHotRestartEventListener implements HazelcastInstanceAware {
        private final AtomicBoolean flag = new AtomicBoolean(false);
        private final boolean partialStart;
        private final boolean onMaster;
        private final int expectedMemberCount;
        private volatile Node node;

        TriggerForceStartListener(boolean partialStart, boolean onMaster, int expectedMemberCount) {
            this.partialStart = partialStart;
            this.onMaster = onMaster;
            this.expectedMemberCount = expectedMemberCount;
        }

        @Override
        public void beforeAllMembersJoin(Collection<? extends Member> currentMembers) {
            if (currentMembers.size() < expectedMemberCount) {
                return;
            }

            if ((onMaster && !node.isMaster()) || (!onMaster && node.isMaster())) {
                return;
            }

            if (!flag.compareAndSet(false, true)) {
                return;
            }

            InternalHotRestartService service = node.getNodeExtension().getInternalHotRestartService();
            if (partialStart) {
                service.triggerPartialStart();
            } else {
                service.triggerForceStart();
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.node = getNode(hazelcastInstance);
        }
    }
}
