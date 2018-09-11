/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.enterprise;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.license.domain.Feature;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.util.RootCauseMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class RollingUpgradeLicenseTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Parameter
    public String license;

    @Parameter(1)
    public String licenseDescription;

    private CompatibilityTestHazelcastInstanceFactory factory;

    @Parameters(name = "{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {SampleLicense.SECURITY_ONLY_LICENSE, "security only"},
                {SampleLicense.UNLIMITED_LICENSE, "unlimited-with-ru"},
                {SampleLicense.UNLIMITED_LICENSE_PRE_ROLLING_UPGRADE, "unlimited-pre-ru"},
        });
    }

    @Before
    public void setup() {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(license);
    }

    @After
    public void tearDown() {
        System.clearProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName());
        factory.terminateAll();
    }

    // test 3.10.2 - 3.10.3 patch level compatibility
    @Test
    public void testPatchLevelCompatibility() {
        String[] versions = new String[] {"3.10.2",
                                          "3.10.2",
                                          CURRENT_VERSION,
                                          CURRENT_VERSION,
                                          "3.10.2"};
        factory = new CompatibilityTestHazelcastInstanceFactory(versions);

        HazelcastInstance[] members = new HazelcastInstance[versions.length];

        // start 3.10.2 cluster
        members[0] = factory.newHazelcastInstance();
        members[1] = factory.newHazelcastInstance();

        testClusterOperates(members);

        // add 3.10.3 members
        members[2] = factory.newHazelcastInstance();
        members[3] = factory.newHazelcastInstance();

        testClusterOperates(members);

        // shutdown 3.10.2 members -> 3.10.3 is master
        members[0].getLifecycleService().terminate();
        members[1].getLifecycleService().terminate();

        // start another 3.10.2 member and make sure it joins the cluster
        members[4] = factory.newHazelcastInstance();

        testClusterOperates(members);
    }

    @Test
    public void testRU_whenMasterIs3_9() {
        String[] versions = new String[] {"3.9.4",
                                          "3.10.2",
                                          CURRENT_VERSION,
                                          "3.9.4"};
        factory = new CompatibilityTestHazelcastInstanceFactory(versions);

        HazelcastInstance[] members = new HazelcastInstance[versions.length];

        // start 3.9 cluster
        members[0] = factory.newHazelcastInstance();
        members[1] = factory.newHazelcastInstance();

        testClusterOperates(members);

        // add 3.10.3 member
        if (!isRollingUpgradeAllowed()) {
            // when RU is not allowed by license, new node fails with IllegalStateException
            // this is wrapped in a GuardianException
            expectedException.expect(new RootCauseMatcher(IllegalStateException.class));
        }
        members[2] = factory.newHazelcastInstance();

        testClusterOperates(members);

        members[0].getLifecycleService().terminate();
        members[1].getLifecycleService().terminate();

        waitAllForSafeState(members[2]);
        members[3] = factory.newHazelcastInstance();

        testClusterOperates(members);
    }

    // smoke test: perform some operations on the cluster
    private void testClusterOperates(HazelcastInstance[] members) {
        for (int i = 1; i < 10; i++) {
            int memberIndex = i % members.length;
            HazelcastInstance member = members[memberIndex];
            if (member == null || !member.getLifecycleService().isRunning()) {
                continue;
            }
            // TODO should be a key generated by member...
            String name = randomString();
            IAtomicLong counter = member.getAtomicLong(name);
            for (int j = 0; j < 100; j++) {
                counter.getAndIncrement();
            }
            assertEquals(100, counter.get());
        }
    }

    private boolean isRollingUpgradeAllowed() {
        License l = LicenseHelper.getLicense(license, BuildInfoProvider.getBuildInfo().getVersion());
        return l.getFeatures().contains(Feature.ROLLING_UPGRADE);
    }
}
