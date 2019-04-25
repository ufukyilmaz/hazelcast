package com.hazelcast.map.impl.operation;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;

public abstract class HDLockAwareOperation extends HDKeyBasedMapOperation implements BlockingOperation {

    protected HDLockAwareOperation() {
    }

    protected HDLockAwareOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    protected HDLockAwareOperation(String name, Data dataKey, long ttl, long maxIdle) {
        super(name, dataKey, ttl, maxIdle);
    }

    protected HDLockAwareOperation(String name, Data dataKey, Data dataValue, long ttl, long maxIdle) {
        super(name, dataKey, dataValue, ttl, maxIdle);
    }

    @Override
    public boolean shouldWait() {
        return !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
    }

    @Override
    public abstract void onWaitExpire();

    @Override
    public final WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DistributedObjectNamespace(MapService.SERVICE_NAME, name), dataKey);
    }
}
