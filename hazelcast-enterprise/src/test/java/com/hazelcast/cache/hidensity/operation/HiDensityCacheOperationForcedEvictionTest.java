package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cache.hidensity.operation.HiDensityCacheOperation.FORCED_EVICTION_RETRY_COUNT;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HiDensityCacheOperationForcedEvictionTest extends AbstractHDCacheOperationTest {

    private static final String CACHE_NAME = "HiDensityCacheOperationForcedEvictionTest";

    @Override
    String getCacheName() {
        return CACHE_NAME;
    }

    /**
     * Triggers a single forced eviction on the RecordStore of the operation.
     */
    @Test
    public void testTryForceEviction() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(1);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEvictionOnLocal(1);
            verifyForcedEvictionOnOthers(0);
            verifyForcedClearOnLocal(0);
            verifyForcedClearOnOthers(0);
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
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEvictionOnLocal(2);
            verifyForcedEvictionOnOthers(0);
            verifyForcedClearOnLocal(0);
            verifyForcedClearOnOthers(0);
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
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEvictionOnLocal(FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictionOnOthers(1);
            verifyForcedClearOnLocal(0);
            verifyForcedClearOnOthers(0);
            verifyForcedEvictionSucceeded();
        }
    }

    /**
     * Triggers a single clear() call on the RecordStore of the operation.
     */
    @Test
    public void testRunWithForcedEviction_evictAll() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * FORCED_EVICTION_RETRY_COUNT + 1);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEvictionOnLocal(FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictionOnOthers(FORCED_EVICTION_RETRY_COUNT);
            verifyForcedClearOnLocal(1);
            verifyForcedClearOnOthers(0);
            verifyForcedEvictionSucceeded();
        }
    }

    /**
     * Triggers a single clear() call on both RecordStores.
     */
    @Test
    public void testRunWithForcedEviction_evictAllOnOthers() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * FORCED_EVICTION_RETRY_COUNT + 2);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEvictionOnLocal(FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictionOnOthers(FORCED_EVICTION_RETRY_COUNT);
            verifyForcedClearOnLocal(1);
            verifyForcedClearOnOthers(1);
            verifyForcedEvictionSucceeded();
        }
    }

    /**
     * Triggers a single clear() call on both RecordStores, but still fails.
     */
    @Test(expected = NativeOutOfMemoryError.class)
    public void testRunWithForcedEviction_withFailedForcedEviction() throws Exception {
        Operation op = new NativeOutOfMemoryOperation(2 * FORCED_EVICTION_RETRY_COUNT + 3);

        try {
            executeOperation(op, PARTITION_ID);
        } finally {
            verifyForcedEvictionOnLocal(FORCED_EVICTION_RETRY_COUNT);
            verifyForcedEvictionOnOthers(FORCED_EVICTION_RETRY_COUNT);
            verifyForcedClearOnLocal(1);
            verifyForcedClearOnOthers(1);
            verifyForcedEvictionFailed();
        }
    }

    private void verifyForcedEvictionOnLocal(int expectedTimes) {
        verify(cacheService, times(expectedTimes)).forceEvict(eq(getCacheName()), anyInt());
    }

    private void verifyForcedEvictionOnOthers(int expectedTimes) {
        verify(cacheService, times(expectedTimes)).forceEvictOnOthers(eq(getCacheName()), anyInt());
    }

    private void verifyForcedClearOnLocal(int expectedTimes) {
        verify(recordStore, times(expectedTimes)).clear();
    }

    private void verifyForcedClearOnOthers(int expectedTimes) {
        verify(cacheService, times(expectedTimes)).clearAll(anyInt());
    }

    private void verifyForcedEvictionSucceeded() {
        verifyDisposeDeferredBlocks(0);
    }

    private void verifyForcedEvictionFailed() {
        verifyDisposeDeferredBlocks(1);
    }

    private void verifyDisposeDeferredBlocks(int expectedTimes) {
        verify(recordStore, times(expectedTimes)).disposeDeferredBlocks();
    }

    private static class NativeOutOfMemoryOperation extends HiDensityCacheOperation {

        private int throwExceptionCounter;

        NativeOutOfMemoryOperation(int throwExceptionCounter) {
            super(CACHE_NAME);
            this.throwExceptionCounter = throwExceptionCounter;
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
