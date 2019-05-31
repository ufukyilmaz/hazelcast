package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.cache.hidensity.operation.AbstractHDCacheOperationTest.OperationType.SET_EXPIRY_POLICY;
import static org.mockito.Mockito.mock;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheSetExpiryPolicyOperationTest extends AbstractHDCacheOperationTest {

    private static final String CACHE_NAME = "CacheSetExpiryPolicyOperationTest";

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
        configureRecordStore(SET_EXPIRY_POLICY);

        Data dataKey = mock(HeapData.class);
        Data dataEP = mock(HeapData.class);

        List<Operation> backupOperations = new ArrayList<>(ENTRY_COUNT);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            CacheSetExpiryPolicyOperation operation = new CacheSetExpiryPolicyOperation(CACHE_NAME, Collections.singletonList(dataKey), dataEP);
            executeOperation(operation, PARTITION_ID);

            if (syncBackupCount > 0) {
                backupOperations.add(operation.getBackupOperation());
            }
        }
        verifyRecordStoreAfterRun(SET_EXPIRY_POLICY, false);
        verifyForcedEviction();

        if (syncBackupCount > 0) {
            for (Operation operation : backupOperations) {
                operation.setPartitionId(PARTITION_ID);
                executeOperation(operation, PARTITION_ID);
            }
            verifyRecordStoreAfterRun(SET_EXPIRY_POLICY, true);
            verifyForcedEviction();
        }
    }

    @Override
    String getCacheName() {
        return CACHE_NAME;
    }
}
