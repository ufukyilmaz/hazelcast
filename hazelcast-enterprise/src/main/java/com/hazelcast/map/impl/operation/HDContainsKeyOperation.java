package com.hazelcast.map.impl.operation;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.WaitNotifyKey;

public class HDContainsKeyOperation extends HDKeyBasedMapOperation implements ReadonlyOperation, BlockingOperation {

    private boolean containsKey;

    public HDContainsKeyOperation() {
    }

    public HDContainsKeyOperation(String name, Data dataKey) {
        super(name, dataKey);
    }


    @Override
    protected void runInternal() {
        containsKey = recordStore.containsKey(dataKey, getCallerAddress());
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        disposeDeferredBlocks();
    }

    @Override
    public Object getResponse() {
        return containsKey;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        DistributedObjectNamespace namespace = new DistributedObjectNamespace(MapService.SERVICE_NAME, name);
        return new LockWaitNotifyKey(namespace, dataKey);
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
    public int getId() {
        return EnterpriseMapDataSerializerHook.CONTAINS_KEY;
    }
}
