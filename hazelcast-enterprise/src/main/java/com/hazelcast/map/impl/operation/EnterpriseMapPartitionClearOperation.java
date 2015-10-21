package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.EnterprisePartitionContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Clears maps partition data. This operation is required because HD backed IMap can only be cleared by
 * corresponding partition threads.
 */
public final class EnterpriseMapPartitionClearOperation
        extends AbstractOperation
        implements PartitionAwareOperation, AllowedDuringPassiveState {

    private final CountDownLatch done = new CountDownLatch(1);

    @Override
    public void run() throws Exception {
        try {
            EnterpriseSerializationService serializationService
                    = (EnterpriseSerializationService) getNodeEngine().getSerializationService();
            MemoryManager memoryManager = serializationService.getMemoryManager();
            if (memoryManager == null || memoryManager.isDestroyed()) {
                // otherwise will cause a SIGSEGV
                return;
            }
            int partitionId = getPartitionId();
            MapService mapService = getService();
            MapServiceContext mapServiceContext = mapService.getMapServiceContext();
            PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
            ((EnterprisePartitionContainer) partitionContainer).clear();
        } finally {
            done.countDown();
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return done.await(timeout, unit);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

}
