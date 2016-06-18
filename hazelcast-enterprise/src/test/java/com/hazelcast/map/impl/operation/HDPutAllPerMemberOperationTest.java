package com.hazelcast.map.impl.operation;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDPutAllPerMemberOperationTest extends AbstractHDOperationTest {

    private static final String MAP_NAME = "HDPutAllPerMemberOperationTest";
    private static final int ITEM_COUNT = 5;
    private static final int PARTITION_COUNT = 2;
    private static final int FIRST_PARTITION_ID = 23;

    private int[] partitions;
    private MapEntries[] mapEntries;

    private NodeEngine nodeEngine;

    @Before
    public void setUp() {
        super.setUp();

        OperationService operationService = new TestOperationService();

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);
        when(nodeEngine.getLogger(anyString())).thenReturn(LOGGER);
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(LOGGER);

        partitions = new int[PARTITION_COUNT];
        mapEntries = new MapEntries[PARTITION_COUNT];

        for (int partitionIndex = 0; partitionIndex < PARTITION_COUNT; partitionIndex++) {
            partitions[partitionIndex] = FIRST_PARTITION_ID + partitionIndex;
            mapEntries[partitionIndex] = createMapEntries(ITEM_COUNT);
        }
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
        configureRecordStore();

        // HDPutAllPerMemberOperation
        HDPutAllPerMemberOperation operation = new HDPutAllPerMemberOperation(MAP_NAME, partitions, mapEntries);
        operation.setNodeEngine(nodeEngine);
        executeMapOperation(operation, 0);

        verifyRecordStoreAfterOperation(true);
        verifyNearCacheInvalidatorAfterOperation();
        verifyHDEvictor();
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

    private class TestOperationService implements OperationService {

        @Override
        public void runOperationOnCallingThread(Operation op) {
            run(op);
        }

        @Override
        public void executeOperation(Operation op) {
            execute(op);
        }

        @Override
        public void run(Operation op) {
        }

        @Override
        public void execute(Operation op) {

        }

        @Override
        public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <E> InternalCompletableFuture<E> invokeOnPartition(Operation op) {
            try {
                assertTrue("Expected an HDPutAllOperation to be invoked", op instanceof HDPutAllOperation);

                HDPutAllOperation operation = (HDPutAllOperation) op;
                executeMapOperation(operation, operation.getPartitionId());
                assertBackupConfiguration(operation);

                if (syncBackupCount > 0) {
                    executeMapOperation(operation.getBackupOperation(), operation.getPartitionId());
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail("Unexpected exception: " + e.getMessage());
            }

            return mock(InternalCompletableFuture.class);
        }

        @Override
        public <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target) {
            return null;
        }

        @Override
        public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId) {
            return null;
        }

        @Override
        public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
            return null;
        }

        @Override
        public Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory) throws Exception {
            return null;
        }

        @Override
        public Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                                       Collection<Integer> partitions) throws Exception {
            return null;
        }

        @Override
        public boolean send(Operation op, Address target) {
            return false;
        }
    }
}
