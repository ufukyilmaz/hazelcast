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

package com.hazelcast.usercodedeployment;

import com.hazelcast.config.Config;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.usercodedeployment.impl.filter.UserCodeDeploymentAbstractTest;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getKnownPreviousVersionsCount;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class UserCodeDeploymentCompatibility_EpAvailableAtOldestVersion_Test extends UserCodeDeploymentAbstractTest {

    @Parameters(name = "ClassCacheMode:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {UserCodeDeploymentConfig.ClassCacheMode.ETERNAL},
                {UserCodeDeploymentConfig.ClassCacheMode.OFF},
        });
    }

    @Parameter
    public volatile UserCodeDeploymentConfig.ClassCacheMode classCacheMode;

    @Override
    protected UserCodeDeploymentConfig.ClassCacheMode getClassCacheMode() {
        return classCacheMode;
    }

    @Override
    protected TestHazelcastInstanceFactory newFactory() {
        return new CompatibilityTestHazelcastInstanceFactory();
    }

    @Test
    @Override
    public void givenSomeMemberCanAccessTheEP_whenTheEPIsFilteredLocally_thenItWillBeLoadedOverNetwork_anonymousInnerClasses() {
        assumeTrue(Versions.PREVIOUS_CLUSTER_VERSION.isGreaterThan(Versions.V3_8));
        super.givenSomeMemberCanAccessTheEP_whenTheEPIsFilteredLocally_thenItWillBeLoadedOverNetwork_anonymousInnerClasses();
    }

    @Override
    protected void executeSimpleTestScenario(Config config, Config epFilteredConfig, EntryProcessor<Integer, Integer> ep) {
        int keyCount = 100;

        CompatibilityTestHazelcastInstanceFactory factory = (CompatibilityTestHazelcastInstanceFactory) newFactory();
        HazelcastInstance instanceWithNewEp = factory.newHazelcastInstance(config);
        factory.newInstances(epFilteredConfig, getKnownPreviousVersionsCount());

        try {
            IMap<Integer, Integer> map = instanceWithNewEp.getMap(randomName());

            for (int i = 0; i < keyCount; i++) {
                map.put(i, 0);
            }
            map.executeOnEntries(ep);
            for (int i = 0; i < keyCount; i++) {
                assertEquals(1, (int) map.get(i));
            }
        } finally {
            factory.shutdownAll();
        }
    }
}
