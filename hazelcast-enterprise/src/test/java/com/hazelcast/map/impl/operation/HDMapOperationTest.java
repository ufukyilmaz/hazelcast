package com.hazelcast.map.impl.operation;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapOperationTest extends AbstractHDMapOperationTest {

    private static final String MAP_NAME = "HDMapOperationTest";

    private TestHDOperation operation;

    @Before
    @Override
    public void setUp() {
        super.setUp();

        operation = new TestHDOperation();
        prepareOperation(operation, PARTITION_ID);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetThreadId() {
        operation.getThreadId();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetThreadId() {
        operation.setThreadId(Thread.currentThread().getId());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testBeforeRun_whenGenericPartitionIdIsSet_thenThrowAssertion() throws Exception {
        operation.setPartitionId(Operation.GENERIC_PARTITION_ID);

        operation.beforeRun();
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
