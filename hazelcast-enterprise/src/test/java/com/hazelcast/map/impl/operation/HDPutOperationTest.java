package com.hazelcast.map.impl.operation;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.map.impl.operation.AbstractHDMapOperationTest.OperationType.PUT;
import static org.mockito.Mockito.mock;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDPutOperationTest extends AbstractHDMapOperationTest {

    private static final String MAP_NAME = "HDPutOperationTest";

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

        // HDPutOperation
        List<Operation> list = new ArrayList<Operation>(ENTRY_COUNT);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            TestHDPutOperation operation = new TestHDPutOperation(MAP_NAME, dataKey, dataValue, 0);
            executeOperation(operation, PARTITION_ID);

            if (syncBackupCount > 0) {
                list.add(operation.getBackupOperation());
            }
        }
        verifyRecordStoreAfterRun(PUT, false);
        verifyHDEvictor(PUT);

        // HDPutBackupOperation
        if (syncBackupCount > 0) {
            for (Operation operation : list) {
                executeOperation(operation, PARTITION_ID);
            }
            verifyRecordStoreAfterRun(PUT, true);
            verifyHDEvictor(PUT);
        }
    }

    @Override
    String getMapName() {
        return MAP_NAME;
    }

    public class TestHDPutOperation extends PutOperation {

        public TestHDPutOperation() {
        }

        public TestHDPutOperation(String name, Data dataKey, Data value, long ttl) {
            super(name, dataKey, value, ttl, -1);
        }
    }
}
