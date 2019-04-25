package com.hazelcast.map.impl.operation;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.WaitNotifyKey;

public final class HDGetOperation extends HDKeyBasedMapOperation
        implements BlockingOperation, ReadonlyOperation {

    private Data result;

    public HDGetOperation() {
    }

    public HDGetOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    @Override
    protected void runInternal() {
        Object record = recordStore.get(dataKey, false, getCallerAddress());
        if (!executedLocally() && record instanceof Data) {
            // in case of a 'remote' call (e..g a client call) we prevent making an onheap copy of the offheap data
            result = (Data) record;
        } else {
            // in case of a local call, we do make a copy so we can safely share it with e.g. near cache invalidation
            result = mapService.getMapServiceContext().toData(record);
        }
    }

    @Override
    public void afterRun() {
        mapServiceContext.interceptAfterGet(name, result);

        disposeDeferredBlocks();
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DistributedObjectNamespace(MapService.SERVICE_NAME, name), dataKey);
    }

    @Override
    public boolean shouldWait() {
        if (recordStore.isTransactionallyLocked(dataKey)) {
            return !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
        }
        return false;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.GET;
    }
}
