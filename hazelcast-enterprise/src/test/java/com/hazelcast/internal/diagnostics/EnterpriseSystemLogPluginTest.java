package com.hazelcast.internal.diagnostics;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.version.Version;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;

/**
 * Extends SystemLogPluginTest including test for logging of cluster version change.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseSystemLogPluginTest extends SystemLogPluginTest {

    @Test
    public void testClusterVersionChange() {
        MemberVersion currentVersion = getNode(hz).getVersion();
        Version nextMinorVersion = Version.of(currentVersion.getMajor(), currentVersion.getMinor() + 1);
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, nextMinorVersion.toString());
        HazelcastInstance instance = hzFactory.newHazelcastInstance(config);
        waitAllForSafeState(hz, instance);
        hz.shutdown();
        waitAllForSafeState(instance);
        getClusterService(instance).changeClusterVersion(nextMinorVersion);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                plugin.run(logWriter);
                assertContains("ClusterVersionChanged");
            }
        });
        System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
    }
}
