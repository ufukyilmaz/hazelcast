package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceV1;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class})
public class HDMapMetadataOOMTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private IMap map;

    @Before
    public void setUp() {
        Config config = getConfig();
        config.getMetricsConfig().setEnabled(false);
        MapConfig mapConfig = new MapConfig()
                .setName("meta")
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(120, MemoryUnit.MEGABYTES))
                .setMetadataSpacePercentage(1)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED);

        config.addMapConfig(mapConfig)
                .setNativeMemoryConfig(memoryConfig)
                .setLicenseKey(UNLIMITED_LICENSE);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "10001");

        HazelcastInstance node = createHazelcastInstance(config);
        map = node.getMap("meta");
    }

    @Test
    public void no_metadata_oome() {
        expectedException.expect(not(containsString("No sufficient memory to allocate metadata")));
        try {
            for (int i = 0; i < 1_000_000; i++) {
                map.set(i, i);
            }
        } finally {
            printMemoryStatus();
        }
    }

    @Test
    public void data_space_oome() {
        expectedException.expectMessage("Not enough contiguous memory available!");
        try {
            for (int i = 0; i < 1_000_000; i++) {
                map.set(i, i);
            }
        } finally {
            printMemoryStatus();
        }
    }

    private void printMemoryStatus() {
        MapServiceContext mapServiceContext = ((MapService) ((MapProxyImpl) map).getService())
                .getMapServiceContext();
        HiDensityStorageInfo hdStorageInfo = ((EnterpriseMapContainer) mapServiceContext
                .getMapContainer(map.getName())).getHDStorageInfo();

        HazelcastMemoryManager memoryManager = ((EnterpriseSerializationServiceV1) mapServiceContext
                .getNodeEngine().getSerializationService()).getMemoryManager();
        MemoryStats memoryStats = memoryManager.getMemoryStats();

        String msg = String.format("Max Native Memory: %s"
                        + ", Committed Native Memory: %s"
                        + ", Used Native Memory: %s"
                        + ", Free Native Memory: %s"
                        + ", Max Metadata: %s"
                        + ", Used Metadata: %s"
                        + ", Free System: %s"
                        + ", IMap Used: %s",
                MemorySize.toPrettyString(memoryStats.getMaxNative()),
                MemorySize.toPrettyString(memoryStats.getCommittedNative()),
                MemorySize.toPrettyString(memoryStats.getUsedNative()),
                MemorySize.toPrettyString(memoryStats.getFreeNative()),
                MemorySize.toPrettyString(memoryStats.getMaxMetadata()),
                MemorySize.toPrettyString(memoryStats.getUsedMetadata()),
                MemorySize.toPrettyString(memoryStats.getFreePhysical()),
                MemorySize.toPrettyString(hdStorageInfo.getUsedMemory()));
        System.err.println(msg);
    }
}
