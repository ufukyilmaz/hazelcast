package com.hazelcast.map.impl.operation;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapOperationTest extends AbstractHDOperationTest {

    private static final String MAP_NAME = "HDMapOperationTest";
    private static final int ITEM_COUNT = 5;
    private static final int PARTITION_COUNT = 3;

    private TestHDOperation operation;

    @Before
    public void setUp() {
        super.setUp();

        operation = new TestHDOperation();
        prepareOperation(operation);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetThreadId() {
        operation.getThreadId();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetThreadId() {
        operation.setThreadId(Operation.GENERIC_PARTITION_ID);
    }

    @Test
    public void testLogError_withNormalException() {
        operation.logError(new RuntimeException("expected exception"));
    }

    @Test
    public void testLogError_withNativeOutOfMemoryError() {
        operation.logError(new NativeOutOfMemoryError("expected exception"));
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

    private static class TestHDOperation extends HDMapOperation {

        @Override
        protected void runInternal() {
        }

        @Override
        public int getId() {
            return 0;
        }
    }
}
