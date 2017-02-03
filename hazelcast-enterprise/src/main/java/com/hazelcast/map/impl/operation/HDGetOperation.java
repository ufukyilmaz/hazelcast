package com.hazelcast.map.impl.operation;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.WaitNotifyKey;

public final class HDGetOperation extends HDKeyBasedMapOperation
        implements IdentifiedDataSerializable, BlockingOperation, ReadonlyOperation {

    private Data result;

    public HDGetOperation() {
    }

    public HDGetOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    @Override
    protected void runInternal() {
        result = mapServiceContext.toData(recordStore.get(dataKey, false));
    }

    @Override
    public void afterRun() {
        mapServiceContext.interceptAfterGet(name, result);

        disposeDeferredBlocks();
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DefaultObjectNamespace(MapService.SERVICE_NAME, name), dataKey);
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
