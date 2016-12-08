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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;
import static com.hazelcast.test.TestClusterUpgradeUtils.upgradeClusterMembers;
import static org.junit.Assert.assertEquals;

/**
 * Create a cluster, then change cluster version.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class})
public class MockJoinerClusterUpgradeTest extends AbstractClusterUpgradeTest {

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(10);

    @Override
    HazelcastInstance createHazelcastInstance(MemberVersion version, Config config) {
        return newHazelcastInstance(factory, version, config);
    }

    @Override
    void upgradeInstances(HazelcastInstance[] instances, MemberVersion version, Config config) {
        upgradeClusterMembers(factory, instances, version, config);
    }

    void assertNodesVersion(MemberVersion version) {
        for (int i=0; i < CLUSTER_MEMBERS_COUNT; i++) {
            assertEquals(version, getNode(clusterMembers[i]).getVersion());
        }
    }

    @After
    public void tearDown() {
        System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
    }
}
