package com.hazelcast.internal.diagnostics;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.test.Accessors.getClusterService;
import static com.hazelcast.test.Accessors.getNode;

/**
 * Extends SystemLogPluginTest including test for logging of cluster version change.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseSystemLogPluginTest extends SystemLogPluginTest {

    @After
    public void tearDown() {
        System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
    }

    @Test
    public void testClusterVersionChange() {
        MemberVersion currentVersion = getNode(hz).getVersion();
        Version nextMinorVersion = Version.of(currentVersion.getMajor(), currentVersion.getMinor() + 1);
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, nextMinorVersion.toString());

        HazelcastInstance instance = hzFactory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, instance);
        waitAllForSafeState(hz, instance);
        hz.shutdown();
        assertClusterSizeEventually(1, instance);
        waitAllForSafeState(instance);
        getClusterService(instance).changeClusterVersion(nextMinorVersion);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                plugin.run(logWriter);
                assertContains("ClusterVersionChanged");
            }
        });
    }
}
