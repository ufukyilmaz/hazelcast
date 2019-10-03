package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

/**
 * Extends SystemLogPluginTest including test for logging of cluster version change.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseProbeTest extends AbstractDiagnosticsPluginTest {

    private MetricsPlugin plugin;
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = getHDConfig();
                config.getMetricsConfig()
                .setMetricsForDataStructuresEnabled(true)
                .setCollectionIntervalSeconds(1);
        hz = createHazelcastInstance(config);

        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);
        plugin = new MetricsPlugin(nodeEngineImpl);
        plugin.onStart();
    }

    @After
    public void tearDown() {
        Diagnostics diagnostics = getDiagnostics(hz);
        cleanupDiagnosticFiles(diagnostics);
    }

    @Test
    public void testHDStorageProbes() {
        IMap<String, String> map = hz.getMap("default");

        map.put("key", "value");
        map.put("key2", "value2");
        plugin.run(logWriter);
        assertContains("[name=default,unit=count,metric=map.entryCount]=2");
        assertContains("[name=default,unit=count,metric=map.forceEvictionCount]=");
        assertContains("[name=default,unit=count,metric=map.usedMemory]=");

        map.remove("key2");
        plugin.run(logWriter);
        assertContains("[name=default,unit=count,metric=map.entryCount]=1");

        map.put("key", "changed_value");
        plugin.run(logWriter);
        assertContains("[name=default,unit=count,metric=map.entryCount]=1");

        map.clear();
        plugin.run(logWriter);
        assertContains("[name=default,unit=count,metric=map.entryCount]=0");

        map.put("new_key", "new_value");
        map.destroy();
        plugin.run(logWriter);
        assertContains("[name=default,unit=count,metric=map.entryCount]=0");
    }
}
