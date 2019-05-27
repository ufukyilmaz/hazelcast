package com.hazelcast.map.impl.operation;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.operation.AbstractHDMapOperationTest.OperationType.PUT_ALL;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDPutAllOperationTest extends AbstractHDMapOperationTest {

    private static final String MAP_NAME = "HDPutAllOperationTest";

    private MapEntries mapEntries;

    @Before
    @Override
    public void setUp() {
        super.setUp();

        mapEntries = createMapEntries(ENTRY_COUNT);
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
        TestHDPutAllOperation operation = new TestHDPutAllOperation(MAP_NAME, mapEntries);
        executeOperation(operation, PARTITION_ID);
        assertBackupConfiguration(operation);

        verifyRecordStoreAfterRun(PUT_ALL, false);
        verifyNearCacheInvalidatorAfterRun();
        verifyHDEvictor(PUT_ALL);

        // HDPutAllBackupOperation
        if (syncBackupCount > 0) {
            executeOperation(operation.getBackupOperation(), PARTITION_ID);

            verifyRecordStoreAfterRun(PUT_ALL, true);
            verifyNearCacheInvalidatorAfterRun();
            verifyHDEvictor(PUT_ALL);
        }
    }

    @Override
    String getMapName() {
        return MAP_NAME;
    }

    public class TestHDPutAllOperation extends PutAllOperation {

        public TestHDPutAllOperation() {
        }

        public TestHDPutAllOperation(String name, MapEntries mapEntries) {
            super(name, mapEntries);
        }
    }
}
