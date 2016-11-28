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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Create a cluster configured with multicast joiner, then change cluster version.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({NightlyTest.class})
public class MulticastJoinerClusterUpgradeTest extends AbstractClusterUpgradeTest {

    @Override
    HazelcastInstance createHazelcastInstance(MemberVersion version, Config config) {
        System.setProperty("hazelcast.version", version.toString());
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        System.clearProperty("hazelcast.version");
        return hz;
    }

    @Override
    void upgradeInstances(final HazelcastInstance[] instances, MemberVersion version, Config config) {
        try {
            // upgrade one by one each member of the cluster to the next version
            for (int i = 0; i < instances.length; i++) {
                instances[i].shutdown();
                waitAllForSafeState(instances);
                // if new node's version is incompatible, then node startup will fail with IllegalStateException
                instances[i] = createHazelcastInstance(version, config);
                waitAllForSafeState(instances);
                // assert all members are in the cluster
                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run()
                            throws Exception {
                        assertEquals(instances.length, instances[0].getCluster().getMembers().size());
                    }
                }, 15);
            }
        }
        finally {
            System.clearProperty("hazelcast.version");
        }
    }

    void assertNodesVersion(MemberVersion version) {
        for (int i=0; i < CLUSTER_MEMBERS_COUNT; i++) {
            assertEquals(version, getNode(clusterMembers[i]).getVersion());
        }
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        return config;
    }

    @After
    public void tearDown() {
        for (HazelcastInstance hz : clusterMembers) {
            hz.shutdown();
        }
        System.clearProperty("hazelcast.version");
    }
}
