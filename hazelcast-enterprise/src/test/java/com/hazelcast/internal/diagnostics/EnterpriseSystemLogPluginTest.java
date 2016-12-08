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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.ClusterVersion;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Extends SystemLogPluginTest including test for logging of cluster version change.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseSystemLogPluginTest extends SystemLogPluginTest {

    private static final String hazelcastVersionSystemPropName = "hazelcast.internal.override.version";

    @Test
    public void testClusterVersionChange() {
        MemberVersion currentVersion = getNode(hz).getVersion();
        ClusterVersion nextMinorVersion = new ClusterVersion(currentVersion.getMajor(), currentVersion.getMinor() + 1);
        System.setProperty(hazelcastVersionSystemPropName, nextMinorVersion.toString());
        HazelcastInstance instance = hzFactory.newHazelcastInstance(config);
        waitAllForSafeState();
        hz.shutdown();
        waitAllForSafeState();
        getClusterService(instance).changeClusterVersion(nextMinorVersion);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                plugin.run(logWriter);
                assertContains("ClusterVersionChanged");
            }
        });
        System.clearProperty(hazelcastVersionSystemPropName);
    }
}
