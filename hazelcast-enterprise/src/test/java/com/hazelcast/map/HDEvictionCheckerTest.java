package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.Random;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE;
import static com.hazelcast.config.MaxSizePolicy.PER_NODE;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDEvictionCheckerTest extends HazelcastTestSupport {

    @Rule
    public TemporaryFolder hotRestartFolder = new TemporaryFolder();

    @Test
    public void testMapEvicted_when_used_native_memory_size_exceeded() throws Exception {
        Config config = newConfig(USED_NATIVE_MEMORY_SIZE, 10);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap("default");

        map.put(1, newValueInMegaBytes(10));

        assertEquals(0, map.size());
    }

    @Test
    public void testMapNotEvicted_when_used_native_memory_size_not_exceeded() throws Exception {
        Config config = newConfig(USED_NATIVE_MEMORY_SIZE, 10);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap("default");

        map.put(1, newValueInMegaBytes(5));

        assertEquals(1, map.size());
    }

    @Test
    public void testMapEvicted_when_free_native_memory_size_exceeded() throws Exception {
        Config config = newConfig(FREE_NATIVE_MEMORY_SIZE, 1000);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap("default");

        map.put(1, newValueInMegaBytes(10));

        assertEquals(0, map.size());
    }

    @Test
    public void testMapNotEvicted_when_free_native_memory_size_not_exceeded() throws Exception {
        Config config = newConfig(FREE_NATIVE_MEMORY_SIZE, 1);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap("default");

        map.put(1, newValueInMegaBytes(10));

        assertEquals(1, map.size());
    }


    @Test
    public void testMapEvicted_when_free_native_memory_percentage_exceeded() throws Exception {
        Config config = newConfig(FREE_NATIVE_MEMORY_PERCENTAGE, 80);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap("default");

        map.put(1, newValueInMegaBytes(10));

        assertEquals(0, map.size());
    }


    @Test
    public void testMapNotEvicted_when_free_native_memory_percentage_not_exceeded() throws Exception {
        Config config = newConfig(FREE_NATIVE_MEMORY_PERCENTAGE, 10);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap("default");

        map.put(1, newValueInMegaBytes(10));

        assertEquals(1, map.size());
    }

    @Test
    public void testHotRestartEnabledMapEvicted_according_to_FREE_NATIVE_MEMORY_PERCENTAGE_whenConfiguredMaxSizePolicyIsDifferent() throws Exception {
        // PER_NODE max-size-policy is configured .
        Config config = newConfig(PER_NODE, Integer.MAX_VALUE);
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        // hot-restart specific config.
        HotRestartPersistenceConfig hrConfig = config.getHotRestartPersistenceConfig();
        hrConfig.setBaseDir(hotRestartFolder.newFolder()).setEnabled(true);
        config.getNativeMemoryConfig().setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED);
        // enable hot restart config on map.
        config.getMapConfig("default").getHotRestartConfig().setEnabled(true);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap("default");

        // Try to put 100 MB into 32 MB native memory.
        for (int i = 0; i < 100; i++) {
            map.put(i, newValueInMegaBytes(1));
        }

        assertTrue("Expecting FREE_NATIVE_MEMORY_PERCENTAGE eviction to kick in", map.size() < 100);
    }

    private Config newConfig(MaxSizePolicy maxSizePolicy, int maxSize) {
        Config config = getHDConfig();

        MapConfig mapConfig = config.getMapConfig("default");
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(maxSizePolicy)
                .setSize(maxSize);

        return config;
    }

    private static byte[] newValueInMegaBytes(int megabytes) {
        Random random = new Random();
        byte[] bytes = new byte[(int) MemoryUnit.MEGABYTES.toBytes(megabytes)];
        random.nextBytes(bytes);
        return bytes;
    }
}
