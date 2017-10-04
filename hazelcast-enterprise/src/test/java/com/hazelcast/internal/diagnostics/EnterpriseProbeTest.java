package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

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
        Config config = getHDConfig()
                .setProperty(Diagnostics.ENABLED.getName(), "true")
                .setProperty(MetricsPlugin.PERIOD_SECONDS.getName(), "1");
        hz = createHazelcastInstance(config);
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);
        plugin = new MetricsPlugin(nodeEngineImpl);
        plugin.onStart();
    }

    @Test
    public void testHDStorageProbes() throws IOException {
        IMap<String, String> map = hz.getMap("default");

        map.put("key", "value");
        map.put("key2", "value2");
        plugin.run(logWriter);
        assertContains("map[default].entryCount=2");
        assertContains("map[default].forceEvictionCount=");
        assertContains("map[default].usedMemory=");

        map.remove("key2");
        plugin.run(logWriter);
        assertContains("map[default].entryCount=1");

        map.put("key", "changed_value");
        plugin.run(logWriter);
        assertContains("map[default].entryCount=1");

        map.clear();
        plugin.run(logWriter);
        assertContains("map[default].entryCount=0");

        map.put("new_key", "new_value");
        map.destroy();
        plugin.run(logWriter);
        assertContains("map[default].entryCount=0");
    }
}
