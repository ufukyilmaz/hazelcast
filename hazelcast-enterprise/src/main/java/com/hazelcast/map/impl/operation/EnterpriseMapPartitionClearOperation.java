package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Clears map's partition data. This operation is required because Hi-Density backed IMap can only be cleared by
 * corresponding partition threads.
 */
public final class EnterpriseMapPartitionClearOperation
        extends Operation
        implements PartitionAwareOperation, AllowedDuringPassiveState, IdentifiedDataSerializable {

    private final CountDownLatch done = new CountDownLatch(1);
    private boolean onShutdown;

    /**
     * Do not use this constructor to obtain an instance of this class, introduced to conform to IdentifiedDataserializable
     * conventions.
     */
    public EnterpriseMapPartitionClearOperation() {
    }

    public EnterpriseMapPartitionClearOperation(boolean onShutdown) {
        this.onShutdown = onShutdown;
    }

    @Override
    public void run() throws Exception {
        try {
            int partitionId = getPartitionId();
            MapService mapService = getService();
            MapServiceContext mapServiceContext = mapService.getMapServiceContext();
            PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
            partitionContainer.clear(onShutdown, false);
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

    @Override
    public int getFactoryId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getId() {
        throw new UnsupportedOperationException();
    }
}
