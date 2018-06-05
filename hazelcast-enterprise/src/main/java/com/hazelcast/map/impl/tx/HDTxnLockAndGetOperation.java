package com.hazelcast.map.impl.tx;

import com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook;
import com.hazelcast.map.impl.operation.HDLockAwareOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

/**
 * Transactional lock and get operation.
 */
public class HDTxnLockAndGetOperation extends HDLockAwareOperation implements MutatingOperation {

    private VersionedValue response;
    private String ownerUuid;
    private boolean shouldLoad;
    private boolean blockReads;

    public HDTxnLockAndGetOperation() {
    }

    public HDTxnLockAndGetOperation(String name, Data dataKey, long timeout, long ttl, String ownerUuid,
                                    boolean shouldLoad, boolean blockReads) {
        super(name, dataKey, ttl);
        this.ownerUuid = ownerUuid;
        this.shouldLoad = shouldLoad;
        this.blockReads = blockReads;
        setWaitTimeout(timeout);
    }

    @Override
    protected void runInternal() {
        if (!recordStore.txnLock(getKey(), ownerUuid, getThreadId(), getCallId(), ttl, blockReads)) {
            throw new TransactionException("Transaction couldn't obtain lock.");
        }
        Record record = recordStore.getRecordOrNull(dataKey);
        if (record == null && shouldLoad) {
            record = recordStore.loadRecordOrNull(dataKey, false, getCallerAddress());
        }
        Data value = record == null ? null : mapService.getMapServiceContext().toData(record.getValue());
        response = new VersionedValue(value, record == null ? 0 : record.getVersion());
    }

    @Override
    public boolean shouldWait() {
        return !recordStore.canAcquireLock(dataKey, ownerUuid, getThreadId());
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(ownerUuid);
        out.writeBoolean(shouldLoad);
        out.writeBoolean(blockReads);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ownerUuid = in.readUTF();
        shouldLoad = in.readBoolean();
        blockReads = in.readBoolean();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", thread=").append(getThreadId());
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.TXN_LOCK_AND_GET;
    }
}
