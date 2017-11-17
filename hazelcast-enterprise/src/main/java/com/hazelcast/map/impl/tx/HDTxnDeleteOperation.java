package com.hazelcast.map.impl.tx;

import com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook;
import com.hazelcast.map.impl.operation.HDBaseRemoveOperation;
import com.hazelcast.map.impl.operation.HDRemoveBackupOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

/**
 * Transactional delete operation.
 */
public class HDTxnDeleteOperation extends HDBaseRemoveOperation implements MapTxnOperation {

    private long version;
    private boolean successful;
    private String ownerUuid;

    public HDTxnDeleteOperation() {
    }

    public HDTxnDeleteOperation(String name, Data dataKey, long version) {
        super(name, dataKey);
        this.version = version;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        if (!recordStore.canAcquireLock(dataKey, ownerUuid, threadId)) {
            throw new TransactionException("Cannot acquire lock UUID: " + ownerUuid + ", threadId: " + threadId);
        }
    }

    @Override
    protected void runInternal() {
        recordStore.unlock(dataKey, ownerUuid, getThreadId(), getCallId());
        Record record = recordStore.getRecord(dataKey);
        if (record == null || version == record.getVersion()) {
            dataOldValue = getNodeEngine().toData(recordStore.remove(dataKey));
            successful = dataOldValue != null;
        }
    }

    @Override
    public boolean shouldWait() {
        return false;
    }

    @Override
    public void afterRun() {
        if (successful) {
            super.afterRun();
        }

        disposeDeferredBlocks();
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new HDRemoveBackupOperation(name, dataKey, true);
    }

    @Override
    public void setOwnerUuid(String ownerUuid) {
        this.ownerUuid = ownerUuid;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(version);
        out.writeUTF(ownerUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        version = in.readLong();
        ownerUuid = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", version=").append(version)
                .append(", successful=").append(successful)
                .append(", ownerUuid=").append(ownerUuid);
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.TXN_DELETE;
    }
}
