package com.hazelcast.map.impl.tx;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook;
import com.hazelcast.map.impl.operation.HDBasePutOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

/**
 * An operation to unlock and set (key,value) on the partition .
 */
public class HDTxnSetOperation extends HDBasePutOperation implements MapTxnOperation, MutatingOperation {

    private long version;
    private transient boolean shouldBackup;
    private String ownerUuid;

    public HDTxnSetOperation() {
    }

    public HDTxnSetOperation(String name, Data dataKey, Data value, long version, long ttl) {
        super(name, dataKey, value);
        this.version = version;
        this.ttl = ttl;
    }

    @Override
    public boolean shouldWait() {
        return false;
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
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final EventService eventService = getNodeEngine().getEventService();
        recordStore.unlock(dataKey, ownerUuid, threadId, getCallId());
        Record record = recordStore.getRecordOrNull(dataKey);
        if (record == null || version == record.getVersion()) {
            if (eventService.hasEventRegistration(MapService.SERVICE_NAME, getName())) {
                oldValue = record == null ? null : mapServiceContext.toData(record.getValue());
            }
            eventType = record == null ? EntryEventType.ADDED : EntryEventType.UPDATED;
            recordStore.set(dataKey, dataValue, ttl, maxIdle);
            shouldBackup = true;
        }
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
    public void setOwnerUuid(String ownerUuid) {
        this.ownerUuid = ownerUuid;
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
    protected boolean shouldUnlockKeyOnBackup() {
        return true;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup && recordStore.getRecord(dataKey) != null;
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
                .append(", shouldBackup=").append(shouldBackup)
                .append(", ownerUuid=").append(ownerUuid);
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.TXN_SET;
    }
}
