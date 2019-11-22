package com.hazelcast.map.impl.operation;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.operation.WithForcedEviction.DEFAULT_FORCED_EVICTION_RETRY_COUNT;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapOperationForcedEvictionTest extends HazelcastTestSupport {

    private static final String MAIN_MAP_NAME = "MAIN_MAP";
    private static final String OTHER_MAP_NAME = "OTHER_MAP";

    private IMap<String, String> mainMap;
    private IMap<String, String> otherMap;

    /**
     * Triggers a single forced eviction on the RecordStore of the operation.
     */
    @Test
    public void testRun_noForcedEvictions_on_main_map_when_no_noome_is_thrown() {
        int noomeCountToBeThrown = 0;
        int numOfEntriesToPutIntoMap = 100;

        Map<String, HiDensityStorageInfo> storageInfoByMapName = runTestWith(noomeCountToBeThrown,
                numOfEntriesToPutIntoMap, 0, LFU, LFU, NATIVE, NATIVE);

        HiDensityStorageInfo hdStorageInfo = storageInfoByMapName.get(MAIN_MAP_NAME);

        assertEquals(0, hdStorageInfo.getForceEvictionCount());
        assertEquals(0, hdStorageInfo.getForceEvictedEntryCount());
        assertEquals(numOfEntriesToPutIntoMap, mainMap.size());
    }

    private static HiDensityStorageInfo getHiDensityStorageInfoOfMap(String mapName, HazelcastInstance node) {
        MapService mapService = getNodeEngineImpl(node).getService(MapService.SERVICE_NAME);
        MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(mapName);
        return ((EnterpriseMapContainer) mapContainer).getHdStorageInfo();
    }

    /**
     * Triggers a single forced eviction on the RecordStore of the operation.
     */
    @Test
    public void testRun_singleForcedEviction_on_main_map_when_one_noome_is_thrown() {
        int noomeCountToBeThrown = 1;
        int numOfEntriesToPutIntoMap = 1;

        Map<String, HiDensityStorageInfo> storageInfoByMapName = runTestWith(noomeCountToBeThrown,
                numOfEntriesToPutIntoMap, 0, LFU, LFU, NATIVE, NATIVE);

        HiDensityStorageInfo hdStorageInfo = storageInfoByMapName.get(MAIN_MAP_NAME);


        assertEquals(1, hdStorageInfo.getForceEvictionCount());
        assertEquals(1, hdStorageInfo.getForceEvictedEntryCount());
        assertEquals(0, mainMap.size());
    }

    /**
     * Triggers multiple forced evictions on the RecordStore of the operation.
     */
    @Test
    public void testRun_multipleForcedEvictions_on_main_map() {
        int noomeCountToBeThrown = 2;
        int numOfEntriesToPutIntoMap = 100;

        Map<String, HiDensityStorageInfo> storageInfoByMapName = runTestWith(noomeCountToBeThrown,
                numOfEntriesToPutIntoMap, 0, LFU, LFU, NATIVE, NATIVE);

        HiDensityStorageInfo hdStorageInfo = storageInfoByMapName.get(MAIN_MAP_NAME);

        assertEquals(2, hdStorageInfo.getForceEvictionCount());
    }

    /**
     * Triggers a single forced eviction on the other
     * maps RecordStore in the same partition.
     */
    @Test
    public void testRun_forcedEviction_on_other_maps() {
        int noomeCountToBeThrown = DEFAULT_FORCED_EVICTION_RETRY_COUNT + 1;
        int numOfEntriesToPutIntoMap = 0;
        int otherMapEntryCount = 100;

        Map<String, HiDensityStorageInfo> storageInfoByMapName = runTestWith(noomeCountToBeThrown,
                numOfEntriesToPutIntoMap, otherMapEntryCount, LFU, LFU, NATIVE, NATIVE);

        HiDensityStorageInfo hdStorageInfo = storageInfoByMapName.get(OTHER_MAP_NAME);

        assertEquals(1, hdStorageInfo.getForceEvictionCount());
    }

    /**
     * Triggers a single evictAll() call on the RecordStore of the operation.
     */
    @Test
    public void testRun_singleEvictAll_on_main_map() {
        int noomeCountToBeThrown = 2 * DEFAULT_FORCED_EVICTION_RETRY_COUNT + 1;
        int mainMapEntryCount = 2000;
        int otherMapEntryCount = 0;

        Map<String, HiDensityStorageInfo> storageInfoByMapName = runTestWith(noomeCountToBeThrown,
                mainMapEntryCount, otherMapEntryCount, LFU, LFU, NATIVE, NATIVE);

        HiDensityStorageInfo hdStorageInfo = storageInfoByMapName.get(MAIN_MAP_NAME);

        assertEquals(2 * DEFAULT_FORCED_EVICTION_RETRY_COUNT, hdStorageInfo.getForceEvictionCount());
        assertEquals(0, mainMap.size());
    }

    /**
     * Triggers a single evictAll() call on both RecordStores.
     */
    @Test
    public void testRun_evictAll_on_other_maps() {
        int noomeCountToBeThrown = 2 * DEFAULT_FORCED_EVICTION_RETRY_COUNT + 2;
        int mainMapEntryCount = 1000;
        int otherMapEntryCount = 1000;

        Map<String, HiDensityStorageInfo> storageInfoByMapName = runTestWith(noomeCountToBeThrown,
                mainMapEntryCount, otherMapEntryCount, LFU, LFU, NATIVE, NATIVE);

        HiDensityStorageInfo mainMapHdStorageInfo = storageInfoByMapName.get(MAIN_MAP_NAME);
        HiDensityStorageInfo otherMapHdStorageInfo = storageInfoByMapName.get(OTHER_MAP_NAME);

        assertEquals(2 * DEFAULT_FORCED_EVICTION_RETRY_COUNT, mainMapHdStorageInfo.getForceEvictionCount());
        assertEquals(DEFAULT_FORCED_EVICTION_RETRY_COUNT, otherMapHdStorageInfo.getForceEvictionCount());
        assertEquals(0, mainMap.size());
        assertEquals(0, otherMap.size());
    }

    /**
     * Triggers a single evictAll() call on the RecordStore of the operation.
     */
    @Test
    public void testRun_evictAllOnOthers_whenOtherRecordStoreHasNoEviction() {
        int noomeCountToBeThrown = 2 * DEFAULT_FORCED_EVICTION_RETRY_COUNT + 2;
        int mainMapEntryCount = 1000;
        int otherMapEntryCount = 1000;

        Map<String, HiDensityStorageInfo> storageInfoByMapName = runTestWith(noomeCountToBeThrown,
                mainMapEntryCount, otherMapEntryCount, LFU, NONE, NATIVE, NATIVE);

        HiDensityStorageInfo mainMapHdStorageInfo = storageInfoByMapName.get(MAIN_MAP_NAME);
        HiDensityStorageInfo otherMapHdStorageInfo = storageInfoByMapName.get(OTHER_MAP_NAME);

        assertEquals(2 * DEFAULT_FORCED_EVICTION_RETRY_COUNT, mainMapHdStorageInfo.getForceEvictionCount());
        assertEquals(0, otherMapHdStorageInfo.getForceEvictionCount());
        assertEquals(0, mainMap.size());
        assertEquals(otherMapEntryCount, otherMap.size());
    }

    /**
     * Triggers a single evictAll() call on both RecordStores, but still fails.
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRun_failedForcedEviction() {
        int noomeCountToBeThrown = 2 * DEFAULT_FORCED_EVICTION_RETRY_COUNT + 3;
        int mainMapEntryCount = 1000;
        int otherMapEntryCount = 1000;

        runTestWith(noomeCountToBeThrown, mainMapEntryCount, otherMapEntryCount,
                LFU, LFU, NATIVE, NATIVE);
    }

    /**
     * Triggers a single evictAll() call on the RecordStore of the operation.
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRun_failedForcedEviction_whenOtherRecordStoreHasNoEviction() {
        int noomeCountToBeThrown = Integer.MAX_VALUE;
        int mainMapEntryCount = 1000;
        int otherMapEntryCount = 1000;

        runTestWith(noomeCountToBeThrown, mainMapEntryCount, otherMapEntryCount,
                LFU, NONE, NATIVE, NATIVE);

    }

    /**
     * Triggers no forced eviction on the other RecordStore, only on the local (which is always NATIVE).
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRun_failedForcedEviction_whenOtherRecordStoreIsNotNativeInMemoryFormat() {
        int noomeCountToBeThrown = Integer.MAX_VALUE;
        int mainMapEntryCount = 1000;
        int otherMapEntryCount = 1000;

        runTestWith(noomeCountToBeThrown, mainMapEntryCount, otherMapEntryCount,
                LFU, NONE, NATIVE, BINARY);
    }

    /**
     * Triggers no forced eviction on the RecordStore of the operation, when it's not created yet
     * and {@link MapOperation#createRecordStoreOnDemand} is {@code false}.
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRun_failedForcedEviction_whenRecordStoreIsNull() {
        int noomeCountToBeThrown = Integer.MAX_VALUE;
        int mainMapEntryCount = 1000;
        int otherMapEntryCount = 1000;

        runTestWith(noomeCountToBeThrown, mainMapEntryCount, otherMapEntryCount,
                LFU, NONE, NATIVE, NATIVE, false);
    }

    private Map<String, HiDensityStorageInfo> runTestWith(int noomeCountToBeThrown,
                                                          int mainMapEntryCount,
                                                          int otherMapEntryCount,
                                                          EvictionPolicy mainMapEvictionPolicy,
                                                          EvictionPolicy otherMapEvictionPolicy,
                                                          InMemoryFormat mainMapFormat,
                                                          InMemoryFormat otherMapFormat) {
        return runTestWith(noomeCountToBeThrown, mainMapEntryCount, otherMapEntryCount,
                mainMapEvictionPolicy, otherMapEvictionPolicy, mainMapFormat, otherMapFormat, true);
    }

    /**
     * Run tests according to given params.
     *
     * @param noomeCountToBeThrown      number of {@link NativeOutOfMemoryError}
     *                                  to be thrown by {@link NativeOutOfMemoryOperation}
     * @param mainMapEntryCount         populate map with this number of entries
     * @param otherMapEntryCount
     * @param mainMapEvictionPolicy     preferred eviction policy
     * @param otherMapEvictionPolicy
     * @param createRecordStoreOnDemand
     * @return {@link HiDensityStorageInfo} which wil
     * be used to check forced eviction statistics
     */
    private Map<String, HiDensityStorageInfo> runTestWith(int noomeCountToBeThrown,
                                                          int mainMapEntryCount,
                                                          int otherMapEntryCount,
                                                          EvictionPolicy mainMapEvictionPolicy,
                                                          EvictionPolicy otherMapEvictionPolicy,
                                                          InMemoryFormat mainMapFormat,
                                                          InMemoryFormat otherMapFormat,
                                                          boolean createRecordStoreOnDemand) {
        // main map's config
        MapConfig mainMapConfig = new MapConfig(MAIN_MAP_NAME);
        mainMapConfig.setInMemoryFormat(mainMapFormat)
                .getEvictionConfig()
                .setEvictionPolicy(mainMapEvictionPolicy);
        // other map's config
        MapConfig otherMapConfig = new MapConfig(OTHER_MAP_NAME);
        otherMapConfig.setInMemoryFormat(otherMapFormat)
                .getEvictionConfig()
                .setEvictionPolicy(otherMapEvictionPolicy);

        Config hdConfig = HDTestSupport.getHDConfig();
        hdConfig.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        hdConfig.addMapConfig(mainMapConfig);
        hdConfig.addMapConfig(otherMapConfig);

        HazelcastInstance node = createHazelcastInstance(hdConfig);
        this.mainMap = node.getMap(MAIN_MAP_NAME);

        // populate maps
        for (int i = 0; i < mainMapEntryCount; i++) {
            this.mainMap.set("key::" + i, "value::" + i);
        }

        if (otherMapEntryCount > 0) {
            otherMap = node.getMap(OTHER_MAP_NAME);
            for (int i = 0; i < otherMapEntryCount; i++) {
                otherMap.set("key::" + i, "value::" + i);
            }
        }

        // run operation
        try {
            Operation operation = new NativeOutOfMemoryOperation(MAIN_MAP_NAME,
                    noomeCountToBeThrown, createRecordStoreOnDemand);

            getOperationService(node)
                    .createInvocationBuilder(MapService.SERVICE_NAME, operation, 0)
                    .invoke().join();
        } catch (CompletionException e) {
            throw ExceptionUtil.rethrow(e.getCause());
        }

        // prepare and return storage info by map name
        Map<String, HiDensityStorageInfo> storageInfoByMapName = new HashMap<>();
        storageInfoByMapName.put(MAIN_MAP_NAME, getHiDensityStorageInfoOfMap(MAIN_MAP_NAME, node));
        storageInfoByMapName.put(OTHER_MAP_NAME, getHiDensityStorageInfoOfMap(OTHER_MAP_NAME, node));
        return storageInfoByMapName;
    }

    private static class NativeOutOfMemoryOperation extends MapOperation {


        private int throwExceptionCounter;

        NativeOutOfMemoryOperation(String mapName,
                                   int throwExceptionCounter,
                                   boolean createRecordStoreOnDemand) {
            super(mapName);
            this.throwExceptionCounter = throwExceptionCounter;
            this.createRecordStoreOnDemand = createRecordStoreOnDemand;
            // we skip the normal afterRun() method, since it always triggers disposeDeferredBlocks(),
            // but we want to use this as test if the NativeOutOfMemoryError was finally thrown or not
            this.disposeDeferredBlocks = false;
        }

        @Override
        public void runInternal() {
            if (throwExceptionCounter-- > 0) {
                throw new NativeOutOfMemoryError("Expected NativeOutOfMemoryError");
            }
        }

        @Override
        public int getClassId() {
            return 0;
        }
    }
}
