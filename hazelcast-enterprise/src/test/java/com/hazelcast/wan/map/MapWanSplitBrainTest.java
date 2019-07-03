package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.impl.WanReplicationPublisherDelegate;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.CountingWanEndpoint;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Asserts that the map merge operation will publish WAN events.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapWanSplitBrainTest extends SplitBrainTestSupport {

    private static final String WAN_REPLICATION_NAME = "wanRef";
    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Parameters(name = "inMemoryFormat:{0} mapMergePolicy:{1} wanMergePolicy:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {OBJECT, com.hazelcast.map.merge.PassThroughMergePolicy.class, PutIfAbsentMapMergePolicy.class},
                {OBJECT, com.hazelcast.spi.merge.PassThroughMergePolicy.class, PutIfAbsentMergePolicy.class},

                {BINARY, com.hazelcast.map.merge.PassThroughMergePolicy.class, PutIfAbsentMapMergePolicy.class},
                {BINARY, com.hazelcast.spi.merge.PassThroughMergePolicy.class, PutIfAbsentMergePolicy.class},

                {NATIVE, com.hazelcast.spi.merge.PassThroughMergePolicy.class, com.hazelcast.spi.merge.PutIfAbsentMergePolicy.class},
                {NATIVE, com.hazelcast.spi.merge.PassThroughMergePolicy.class, com.hazelcast.spi.merge.PutIfAbsentMergePolicy.class},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public Class mapMergePolicy;

    @Parameter(value = 2)
    public Class wanMergePolicy;

    private String mapName = randomString();
    private IMap<String, String> map1;
    private IMap<String, String> map2;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mapMergePolicy.getName());

        WanReplicationRef wanReplicationRef = new WanReplicationRef()
                .setName(WAN_REPLICATION_NAME)
                .setMergePolicy(wanMergePolicy.getName());

        MapConfig mapConfig = new MapConfig()
                .setInMemoryFormat(inMemoryFormat)
                .setName(mapName)
                .setMergePolicyConfig(mergePolicyConfig)
                .setWanReplicationRef(wanReplicationRef);

        WanReplicationConfig wanConfig = new WanReplicationConfig()
                .setName(WAN_REPLICATION_NAME)
                .addWanPublisherConfig(new WanPublisherConfig()
                        .setClassName(CountingWanEndpoint.class.getName()));

        return getHDConfig(super.config(), POOLED, MEMORY_SIZE)
                .addMapConfig(mapConfig)
                .addWanReplicationConfig(wanConfig);
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        warmUpPartitions(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        map1 = firstBrain[0].getMap(mapName);
        map2 = secondBrain[0].getMap(mapName);

        map1.put("key", "value");
        map2.put("key", "passThroughValue");
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        assertEquals("passThroughValue", map1.get("key"));
        assertEquals("passThroughValue", map2.get("key"));

        int totalPublishedEvents = 0;
        int totalPublishedBackupEvents = 0;
        for (HazelcastInstance instance : instances) {
            EnterpriseWanReplicationService wanReplicationService
                    = (EnterpriseWanReplicationService) getNodeEngineImpl(instance).getWanReplicationService();
            WanReplicationPublisherDelegate delegate
                    = (WanReplicationPublisherDelegate) wanReplicationService.getWanReplicationPublisher(WAN_REPLICATION_NAME);
            for (WanReplicationEndpoint endpoint : delegate.getEndpoints()) {
                CountingWanEndpoint countingEndpoint = (CountingWanEndpoint) endpoint;
                totalPublishedEvents += countingEndpoint.getCount();
                totalPublishedBackupEvents += countingEndpoint.getBackupCount();
            }
        }
        assertEquals("Expected 3 published WAN events", 3, totalPublishedEvents);
        assertEquals("Expected 3 published WAN backup events", 3, totalPublishedBackupEvents);
    }
}
