package com.hazelcast.map.impl.operation;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.WaitNotifyKey;

public class HDGetEntryViewOperation extends HDKeyBasedMapOperation implements ReadonlyOperation, BlockingOperation {

    private EntryView<Data, Data> result;

    public HDGetEntryViewOperation() {
    }

    public HDGetEntryViewOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    @Override
    protected void runInternal() {
        Record record = recordStore.getRecordOrNull(dataKey);
        if (record != null) {
            Data value = mapServiceContext.toData(record.getValue());
            result = EntryViews.createSimpleEntryView(dataKey, value, record);
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        disposeDeferredBlocks();
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DistributedObjectNamespace(MapService.SERVICE_NAME, name), dataKey);
    }

    @Override
    public boolean shouldWait() {
        return recordStore.isTransactionallyLocked(dataKey)
                && !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
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
        return EnterpriseMapDataSerializerHook.GET_ENTRY_VIEW;
    }
}
