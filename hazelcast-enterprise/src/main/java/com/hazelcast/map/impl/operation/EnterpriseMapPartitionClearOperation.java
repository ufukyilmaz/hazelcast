package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Clears map's partition data. This operation is
 * required because Hi-Density backed IMap can only
 * be cleared by corresponding partition threads.
 */
public final class EnterpriseMapPartitionClearOperation extends Operation
        implements PartitionAwareOperation, AllowedDuringPassiveState, IdentifiedDataSerializable {

    private final transient boolean onShutdown;
    private final CountDownLatch done = new CountDownLatch(1);

    /**
     * Do not use this constructor to obtain an instance of this class,
     * introduced to conform to IdentifiedDataserializable conventions.
     */
    public EnterpriseMapPartitionClearOperation() {
        this(false);
    }

    public EnterpriseMapPartitionClearOperation(boolean onShutdown) {
        this.onShutdown = onShutdown;
    }

    @Override
    public void run() {
        try {
            int partitionId = getPartitionId();
            MapService mapService = getService();
            MapServiceContext mapServiceContext = mapService.getMapServiceContext();
            mapServiceContext.removeRecordStoresFromPartitionMatchingWith(allRecordStores(),
                    partitionId, onShutdown, false);
        } finally {
            done.countDown();
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        done.countDown();
        super.onExecutionFailure(e);
    }

    /**
     * @return predicate that matches with partitions of all maps
     */
    private static Predicate<RecordStore> allRecordStores() {
        return recordStore -> true;
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
    protected void writeInternal(ObjectDataOutput out) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) {
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
