package com.hazelcast.map.impl.operation;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.operation.HDMapOperation.FORCED_EVICTION_RETRY_COUNT;
import static java.util.Collections.singletonMap;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapOperationForcedEvictionTest extends AbstractHDOperationTest {

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

    @Override
    int getItemCount() {
        return 0;
    }

    @Override
    int getPartitionCount() {
        return 2;
    }

    /**
     * Triggers a single forced eviction on the RecordStore of the operation.
     */
    @Test
    public void testTryForceEviction() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(1);

        try {
            executeMapOperation(op, 1);
        } finally {
            verifyForcedEviction(recordStore, 1);
            verifyForcedEviction(otherRecordStore, 0);
            verifyForcedEvictAll(recordStore, 0);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEvictionSucceeded();
        }
    }

    /**
     * Triggers multiple forced evictions on the RecordStore of the operation.
     */
    @Test
    public void testRunWithForcedEviction_withRetries() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2);

        try {
            executeMapOperation(op, 1);
        } finally {
            verifyForcedEviction(recordStore, 2);
            verifyForcedEviction(otherRecordStore, 0);
            verifyForcedEvictAll(recordStore, 0);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEvictionSucceeded();
        }
    }

    /**
     * Triggers a single forced eviction on the other RecordStore.
     */
    @Test
    public void testRunWithForcedEviction_forceEvictionOnOthers() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(FORCED_EVICTION_RETRY_COUNT + 1);

        try {
            executeMapOperation(op, 1);
        } finally {
            verifyForcedEviction(recordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, 1);
            verifyForcedEvictAll(recordStore, 0);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEvictionSucceeded();
        }
    }

    /**
     * Triggers a single evictAll() call on the RecordStore of the operation.
     */
    @Test
    public void testRunWithForcedEviction_evictAll() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * FORCED_EVICTION_RETRY_COUNT + 1);

        try {
            executeMapOperation(op, 1);
        } finally {
            verifyForcedEviction(recordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEvictionSucceeded();
        }
    }

    /**
     * Triggers a single evictAll() call on both RecordStores.
     */
    @Test
    public void testRunWithForcedEviction_evictAllOnOthers() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * FORCED_EVICTION_RETRY_COUNT + 2);

        try {
            executeMapOperation(op, 1);
        } finally {
            verifyForcedEviction(recordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 1);
            verifyForcedEvictionSucceeded();
        }
    }

    /**
     * Triggers a single evictAll() call on the RecordStore of the operation.
     */
    @Test
    public void testRunWithForcedEviction_evictAllOnOthers_whenOtherRecordStoreHasNoEviction() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * FORCED_EVICTION_RETRY_COUNT + 2);
        otherMapConfig.setEvictionPolicy(EvictionPolicy.NONE);

        try {
            executeMapOperation(op, 1);
        } finally {
            verifyForcedEviction(recordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEvictionSucceeded();
        }
    }

    /**
     * Triggers a single evictAll() call on both RecordStores, but still fails.
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRunWithForcedEviction_withFailedForcedEviction() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * FORCED_EVICTION_RETRY_COUNT + 3);

        try {
            executeMapOperation(op, 1);
        } finally {
            verifyForcedEviction(recordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 1);
            verifyForcedEvictionFailed();
        }
    }

    /**
     * Triggers a single evictAll() call on the RecordStore of the operation.
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRunWithForcedEviction_withFailedForcedEviction_whenOtherRecordStoreHasNoEviction() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * FORCED_EVICTION_RETRY_COUNT + 3);
        otherMapConfig.setEvictionPolicy(EvictionPolicy.NONE);

        try {
            executeMapOperation(op, 1);
        } finally {
            verifyForcedEviction(recordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEvictionFailed();
        }
    }

    /**
     * Triggers no forced eviction on the other RecordStore, only on the local (which is always NATIVE).
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRunWithForcedEviction_whenOtherRecordStoreIsNotNativeInMemoryFormat() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(Integer.MAX_VALUE);
        otherMapConfig.setInMemoryFormat(InMemoryFormat.BINARY);

        try {
            executeMapOperation(op, 1);
        } finally {
            verifyForcedEviction(recordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEviction(otherRecordStore, 0);
            verifyForcedEvictAll(recordStore, 1);
            verifyForcedEvictAll(otherRecordStore, 0);
            verifyForcedEvictionFailed();
        }
    }

    /**
     * Triggers no forced eviction on the RecordStore of the operation, when it's not created yet
     * and {@link MapOperation#createRecordStoreOnDemand} is {@code false}.
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRunWithForcedEviction_whenRecordStoreIsNull() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(Integer.MAX_VALUE, false);

        try {
            executeMapOperation(op, 1);
        } finally {
            verifyForcedEviction(recordStore, 0);
            verifyForcedEviction(otherRecordStore, FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictAll(recordStore, 0);
            verifyForcedEvictAll(otherRecordStore, 1);
            verifyForcedEvictionFailed();
        }
    }

    private void verifyForcedEviction(RecordStore recordStore, int expectedTimes) {
        verify(evictor, times(expectedTimes)).forceEvict(recordStore);
    }

    private void verifyForcedEvictAll(RecordStore recordStore, int expectedTimes) {
        verify(recordStore, times(expectedTimes)).evictAll(false);
    }

    private void verifyForcedEvictionSucceeded() {
        verifyDisposeDeferredBlocks(recordStore, 0);
        verifyDisposeDeferredBlocks(otherRecordStore, 0);
    }

    private void verifyForcedEvictionFailed() {
        verifyDisposeDeferredBlocks(recordStore, 1);
        verifyDisposeDeferredBlocks(otherRecordStore, 0);
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

    private class NativeOutOfMemoryOperation extends HDMapOperation {

        private int throwExceptionCounter;

        NativeOutOfMemoryOperation(int throwExceptionCounter) {
            this(throwExceptionCounter, true);
        }

        NativeOutOfMemoryOperation(int throwExceptionCounter, boolean createRecordStoreOnDemand) {
            super(MAP_NAME);
            this.throwExceptionCounter = throwExceptionCounter;
            this.createRecordStoreOnDemand = createRecordStoreOnDemand;
        }

        @Override
        public void runInternal() {
            if (throwExceptionCounter-- > 0) {
                throw new NativeOutOfMemoryError("Expected NativeOutOfMemoryError");
            }
        }

        @Override
        public void afterRun() {
            // we skip the normal afterRun() method, since it always triggers disposeDeferredBlocks(),
            // but we want to use this as test if the NativeOutOfMemoryError was finally thrown or not
        }

        @Override
        public int getId() {
            return 0;
        }
    }
}
