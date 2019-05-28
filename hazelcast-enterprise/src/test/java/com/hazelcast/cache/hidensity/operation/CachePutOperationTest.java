package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.expiry.ExpiryPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.hidensity.operation.AbstractHDCacheOperationTest.OperationType.PUT;
import static org.mockito.Mockito.mock;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CachePutOperationTest extends AbstractHDCacheOperationTest {

    private static final String CACHE_NAME = "CachePutOperationTest";

    @Test
    public void testRun() throws Exception {
        syncBackupCount = 0;
        throwNativeOOME = false;

        testRunInternal();
    }

    @Test
    public void testRun_shouldWriteAllBackups() throws Exception {
        syncBackupCount = 1;
        throwNativeOOME = false;

        testRunInternal();
    }

    @Test
    public void testRun_whenNativeOutOfMemoryError_thenShouldNotInsertEntriesTwice() throws Exception {
        syncBackupCount = 0;
        throwNativeOOME = true;

        testRunInternal();
    }

    @Test
    public void testRun_whenNativeOutOfMemoryError_thenShouldNotInsertEntriesTwice_shouldWriteAllBackups() throws Exception {
        syncBackupCount = 1;
        throwNativeOOME = true;

        testRunInternal();
    }

    private void testRunInternal() throws Exception {
        configureBackups();
        configureRecordStore(PUT);

        Data dataKey = mock(HeapData.class);
        Data dataValue = mock(HeapData.class);
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1, TimeUnit.HOURS);

        // CachePutOperation
        List<Operation> backupOperations = new ArrayList<Operation>(ENTRY_COUNT);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            CachePutOperation operation = new CachePutOperation(CACHE_NAME, dataKey, dataValue, expiryPolicy, true);
            executeOperation(operation, PARTITION_ID);

            if (syncBackupCount > 0) {
                backupOperations.add(operation.getBackupOperation());
            }
        }
        verifyRecordStoreAfterRun(PUT, false);
        verifyForcedEviction();

        // CachePutBackupOperation
        if (syncBackupCount > 0) {
            for (Operation operation : backupOperations) {
                operation.setPartitionId(PARTITION_ID);
                executeOperation(operation, PARTITION_ID);
            }
            verifyRecordStoreAfterRun(PUT, true);
            verifyForcedEviction();
        }
    }

    @Override
    String getCacheName() {
        return CACHE_NAME;
    }
}
