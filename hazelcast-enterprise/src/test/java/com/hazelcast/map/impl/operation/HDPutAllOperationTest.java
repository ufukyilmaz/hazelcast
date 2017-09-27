package com.hazelcast.map.impl.operation;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.operation.AbstractHDOperationTest.OperationType.PUT_ALL;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDPutAllOperationTest extends AbstractHDOperationTest {

    private static final String MAP_NAME = "HDPutAllOperationTest";
    private static final int ITEM_COUNT = 5;
    private static final int PARTITION_COUNT = 3;
    private static final int PARTITION_ID = 23;

    private MapEntries mapEntries;

    @Before
    @Override
    public void setUp() {
        super.setUp();

        mapEntries = createMapEntries(ITEM_COUNT);
    }

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
        configureRecordStore(PUT_ALL);

        // HDPutAllOperation
        HDPutAllOperation operation = new HDPutAllOperation(MAP_NAME, mapEntries);
        executeMapOperation(operation, PARTITION_ID);
        assertBackupConfiguration(operation);

        verifyRecordStoreAfterOperation(PUT_ALL, false);

        // HDPutAllBackupOperation
        if (syncBackupCount > 0) {
            executeMapOperation(operation.getBackupOperation(), PARTITION_ID);
        }

        verifyRecordStoreAfterOperation(PUT_ALL, true);
        verifyNearCacheInvalidatorAfterOperation();
        verifyHDEvictor(PUT_ALL);
    }

    @Override
    String getMapName() {
        return MAP_NAME;
    }

    @Override
    int getItemCount() {
        return ITEM_COUNT;
    }

    @Override
    int getPartitionCount() {
        return PARTITION_COUNT;
    }
}
