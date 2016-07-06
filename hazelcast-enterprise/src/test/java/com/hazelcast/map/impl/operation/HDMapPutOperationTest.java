package com.hazelcast.map.impl.operation;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.map.impl.operation.AbstractHDOperationTest.OperationType.PUT;
import static org.mockito.Mockito.mock;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapPutOperationTest extends AbstractHDOperationTest {

    private static final String MAP_NAME = "HDMapPutOperationTest";
    private static final int ITEM_COUNT = 5;
    private static final int PARTITION_COUNT = 3;
    private static final int PARTITION_ID = 23;

    @Before
    public void setUp() {
        super.setUp();
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
        Data dataKey = mock(Data.class);
        Data dataValue = mock(Data.class);

        configureBackups();
        configureRecordStore(PUT);

        List<Operation> list = new ArrayList<Operation>(ITEM_COUNT);
        for (int i = 0; i < ITEM_COUNT; i++) {
            HDPutOperation operation = new HDPutOperation(MAP_NAME, dataKey, dataValue, 0);
            prepareOperation(operation);

            executeMapOperation(operation, PARTITION_ID);

            if (syncBackupCount > 0) {
                list.add(operation.getBackupOperation());
            }
        }
        verifyRecordStoreAfterOperation(PUT, false);
        verifyHDEvictor(PUT);

        if (syncBackupCount > 0) {
            for (Operation operation : list) {
                operation.setPartitionId(PARTITION_ID);
                executeMapOperation(operation, PARTITION_ID);
            }
            verifyRecordStoreAfterOperation(PUT, true);
            verifyHDEvictor(PUT);
        }
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
