package com.hazelcast.map.impl.record;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDRecordFactoryTest extends AbstractRecordFactoryTest<Data> {

    private HazelcastMemoryManager memoryManager;

    @After
    public void tearDown() {
        if (memoryManager != null) {
            memoryManager.dispose();
        }
    }

    @Override
    void newRecordFactory(boolean isStatisticsEnabled, CacheDeserializedValues cacheDeserializedValues) {
        MapContainer mapContainer = newMapContainer(isStatisticsEnabled, cacheDeserializedValues);
        factory = new HDRecordFactory(createHiDensityRecordProcessor(mapContainer), mapContainer);
    }

    static MapContainer newMapContainer(boolean isPerEntryStatsEnabled,
                                                CacheDeserializedValues cacheDeserializedValues) {
        NodeEngine nodeEngine = mock(NodeEngine.class);
        ClusterService clusterService = mock(ClusterService.class);
        MapServiceContext mapServiceContext = mock(MapServiceContext.class);

        when(mapServiceContext.getNodeEngine()).thenReturn(nodeEngine);
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
        when(clusterService.getClusterVersion()).thenReturn(Versions.CURRENT_CLUSTER_VERSION);

        MapContainer mapContainer = mock(MapContainer.class);
        MapConfig mapConfig = mock(MapConfig.class);
        when(mapConfig.getCacheDeserializedValues()).thenReturn(cacheDeserializedValues);
        when(mapConfig.isPerEntryStatsEnabled()).thenReturn(isPerEntryStatsEnabled);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.NONE);
        when(mapConfig.getEvictionConfig()).thenReturn(evictionConfig);
        HotRestartConfig hotRestartConfig = new HotRestartConfig().setEnabled(false);
        when(mapConfig.getHotRestartConfig()).thenReturn(hotRestartConfig);
        when(mapContainer.getMapConfig()).thenReturn(mapConfig);
        when(mapContainer.getEvictor()).thenReturn(Evictor.NULL_EVICTOR);
        when(mapContainer.getMapServiceContext()).thenReturn(mapServiceContext);
        return mapContainer;
    }

    @Test
    public void testGetRecordProcessor() {
        HiDensityRecordProcessor<HDRecord> recordProcessor = createHiDensityRecordProcessor(
                newMapContainer(true, CacheDeserializedValues.ALWAYS));

        assertEquals(recordProcessor, new HDRecordFactory(recordProcessor, mock(EnterpriseMapContainer.class)).getRecordProcessor());
    }

    @Test
    public void testIsNull_withNullNativeMemoryData() {
        NativeMemoryData data = new NativeMemoryData(NULL_PTR, 0);

        assertTrue(HDRecordFactory.isNull(data));
    }

    @Test
    public void testIsNull_withNativeMemoryData() {
        EnterpriseSerializationService ess = ((EnterpriseSerializationService) serializationService);

        Person object = new Person("Eve");
        NativeMemoryData data = ess.toNativeData(object, memoryManager);

        assertFalse(HDRecordFactory.isNull(data));
    }

    @Override
    Class<?> getRecordClass() {
        return HDSimpleRecordWithVersion.class;
    }

    @Override
    Class<?> getRecordWithStatsClass() {
        return HDRecordWithStats.class;
    }

    @Override
    Class<?> getCachedRecordClass() {
        return HDSimpleRecordWithVersion.class;
    }

    @Override
    Class<?> getCachedRecordWithStatsClass() {
        return HDRecordWithStats.class;
    }

    @Override
    InternalSerializationService createSerializationService() {
        MemorySize memorySize = new MemorySize(5, MemoryUnit.MEGABYTES);
        memoryManager = new PoolingMemoryManager(memorySize);

        return new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .build();
    }

    @Override
    Record<Data> newRecord(RecordFactory<Data> factory, Object value) {
        return factory.newRecord(value);
    }

    private HiDensityRecordProcessor<HDRecord> createHiDensityRecordProcessor(MapContainer mapContainer) {
        EnterpriseSerializationService ess = (EnterpriseSerializationService) serializationService;
        HiDensityRecordAccessor<HDRecord> recordAccessor = new HDMapRecordAccessor(ess, mapContainer);
        HazelcastMemoryManager memoryManager = ess.getMemoryManager();
        HiDensityStorageInfo storageInfo = new HiDensityStorageInfo("myStorage");
        return new DefaultHiDensityRecordProcessor<>(ess, recordAccessor, memoryManager, storageInfo);
    }
}
