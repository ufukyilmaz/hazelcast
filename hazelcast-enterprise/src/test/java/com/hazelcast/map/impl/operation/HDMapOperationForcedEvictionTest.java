package com.hazelcast.map.impl.operation;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.operation.WithForcedEviction.DEFAULT_FORCED_EVICTION_RETRY_COUNT;
import static java.util.Collections.singletonMap;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapOperationForcedEvictionTest extends AbstractHDMapOperationTest {

    private static final String MAP_NAME = "HDMapOperationForcedEvictionTest";

    private MapConfig otherMapConfig;
    private RecordStore otherRecordStore;

    @Before
    @Override
    public void setUp() {
        super.setUp();

        otherMapConfig = new MapConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionPolicy(EvictionPolicy.RANDOM);

        otherRecordStore = mockRecordStore(evictor, otherMapConfig);

        when(mapService.getMapServiceContext().getExistingRecordStore(geq(1), eq(MAP_NAME))).thenReturn(recordStore);
        partitionMaps.putAll(singletonMap("otherMap", otherRecordStore));
    }

    @Override
    String getMapName() {
        return MAP_NAME;
    }

    /**
     * Triggers a single forced eviction on the RecordStore of the operation.
     */
    @Test
    public void testRun_noForcedEvictions() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(0);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEviction(recordStore, 0);
            verifyForcedEviction(otherRecordStore, 0);
            verifyForcedEvictAll(recordStore, 0);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEviction(0, 0);
        }
    }

    /**
     * Triggers a single forced eviction on the RecordStore of the operation.
     */
    @Test
    public void testRun_singleForcedEviction() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(1);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEviction(recordStore, 1);
            verifyForcedEviction(otherRecordStore, 0);
            verifyForcedEvictAll(recordStore, 0);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEviction(0, 0);
        }
    }

    /**
     * Triggers multiple forced evictions on the RecordStore of the operation.
     */
    @Test
    public void testRun_multipleForcedEvictions() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEviction(recordStore, 2);
            verifyForcedEviction(otherRecordStore, 0);
            verifyForcedEvictAll(recordStore, 0);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEviction(0, 0);
        }
    }

    /**
     * Triggers a single forced eviction on the other RecordStore.
     */
    @Test
    public void testRun_forcedEvictionOnOthers() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(DEFAULT_FORCED_EVICTION_RETRY_COUNT + 1);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEviction(recordStore, DEFAULT_FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, 1);
            verifyForcedEvictAll(recordStore, 0);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEviction(0, 0);
        }
    }

    /**
     * Triggers a single evictAll() call on the RecordStore of the operation.
     */
    @Test
    public void testRun_singleEvictAll() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * DEFAULT_FORCED_EVICTION_RETRY_COUNT + 1);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEviction(recordStore, DEFAULT_FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, DEFAULT_FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEviction(1, 0);
        }
    }

    /**
     * Triggers a single evictAll() call on both RecordStores.
     */
    @Test
    public void testRun_evictAllOnOthers() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * DEFAULT_FORCED_EVICTION_RETRY_COUNT + 2);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEviction(recordStore, DEFAULT_FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, DEFAULT_FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 1);
            verifyForcedEviction(1, 1);
        }
    }

    /**
     * Triggers a single evictAll() call on the RecordStore of the operation.
     */
    @Test
    public void testRun_evictAllOnOthers_whenOtherRecordStoreHasNoEviction() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * DEFAULT_FORCED_EVICTION_RETRY_COUNT + 2);
        otherMapConfig.setEvictionPolicy(EvictionPolicy.NONE);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEviction(recordStore, DEFAULT_FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, 0);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEviction(1, 0);
        }
    }

    /**
     * Triggers a single evictAll() call on both RecordStores, but still fails.
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRun_failedForcedEviction() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * DEFAULT_FORCED_EVICTION_RETRY_COUNT + 3);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEviction(recordStore, DEFAULT_FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, DEFAULT_FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 1);
            verifyForcedEviction(1, 1);
        }
    }

    /**
     * Triggers a single evictAll() call on the RecordStore of the operation.
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRun_failedForcedEviction_whenOtherRecordStoreHasNoEviction() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(Integer.MAX_VALUE);
        otherMapConfig.setEvictionPolicy(EvictionPolicy.NONE);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEviction(recordStore, DEFAULT_FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, 0);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEviction(1, 0);
        }
    }

    /**
     * Triggers no forced eviction on the other RecordStore, only on the local (which is always NATIVE).
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRun_failedForcedEviction_whenOtherRecordStoreIsNotNativeInMemoryFormat() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(Integer.MAX_VALUE);
        otherMapConfig.setInMemoryFormat(InMemoryFormat.BINARY);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEviction(recordStore, DEFAULT_FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, 0);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEviction(1, 0);
        }
    }

    /**
     * Triggers no forced eviction on the RecordStore of the operation, when it's not created yet
     * and {@link MapOperation#createRecordStoreOnDemand} is {@code false}.
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRun_failedForcedEviction_whenRecordStoreIsNull() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(Integer.MAX_VALUE, false);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEviction(recordStore, 0);
            verifyForcedEviction(otherRecordStore, DEFAULT_FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictAll(recordStore, 0);
            verifyForcedEvictAll(otherRecordStore, 1);
            verifyForcedEviction(0, 1);
        }
    }

    private void verifyForcedEviction(RecordStore recordStore, int expectedTimes) {
        verify(evictor, times(expectedTimes)).forceEvict(recordStore);
    }

    private void verifyForcedEvictAll(RecordStore recordStore, int expectedTimes) {
        verify(recordStore, times(expectedTimes)).evictAll(false);
    }

    private void verifyForcedEviction(int onMain, int onOthers) {
        verifyDisposeDeferredBlocks(recordStore, onMain);
        verifyDisposeDeferredBlocks(otherRecordStore, onOthers);
    }

    private void verifyDisposeDeferredBlocks(RecordStore recordStore, int expectedTimes) {
        verify(recordStore, times(expectedTimes)).disposeDeferredBlocks();
    }

    private static RecordStore mockRecordStore(Evictor evictor, MapConfig mapConfig) {
        MapContainer mapContainer = mock(MapContainer.class);
        when(mapContainer.getEvictor()).thenReturn(evictor);
        when(mapContainer.getMapConfig()).thenReturn(mapConfig);

        RecordStore recordStore = mock(RecordStore.class);
        when(recordStore.getMapContainer()).thenReturn(mapContainer);

        return recordStore;
    }

    private class NativeOutOfMemoryOperation extends MapOperation {

        private int throwExceptionCounter;

        NativeOutOfMemoryOperation(int throwExceptionCounter) {
            this(throwExceptionCounter, true);
        }

        NativeOutOfMemoryOperation(int throwExceptionCounter, boolean createRecordStoreOnDemand) {
            super(MAP_NAME);
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
