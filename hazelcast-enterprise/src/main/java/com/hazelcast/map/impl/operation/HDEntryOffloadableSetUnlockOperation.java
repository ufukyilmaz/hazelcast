package com.hazelcast.map.impl.operation;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;

/**
 * HD Version of EntryOffloadableSetUnlockOperation
 * <p>
 * See the javadoc on {@link EntryOperation} for full documentation.
 * <p>
 * GOTCHA : This operation LOADS missing keys from map-store, in contrast with PartitionWideEntryOperation.
 */
public class HDEntryOffloadableSetUnlockOperation extends HDKeyBasedMapOperation implements BackupAwareOperation, Notifier {

    protected Data newValue;
    protected Data oldValue;
    protected String caller;
    protected long begin;
    protected EntryEventType modificationType;
    protected EntryBackupProcessor entryBackupProcessor;

    public HDEntryOffloadableSetUnlockOperation() {
    }

    public HDEntryOffloadableSetUnlockOperation(String name, EntryEventType modificationType, Data key, Data oldValue,
                                                Data newValue, String caller, long threadId, long begin,
                                                EntryBackupProcessor entryBackupProcessor) {
        super(name, key, newValue);
        this.newValue = newValue;
        this.oldValue = oldValue;
        this.caller = caller;
        this.begin = begin;
        this.modificationType = modificationType;
        this.entryBackupProcessor = entryBackupProcessor;
        this.setThreadId(threadId);
    }

    @Override
    public void runInternal() {
        verifyLock();
        try {
            operator(this)
                    .init(dataKey, oldValue, newValue, null, modificationType)
                    .doPostOperateOps();
        } finally {
            unlockKey();
        }
    }

    private void verifyLock() {
        if (!recordStore.isLockedBy(dataKey, caller, threadId)) {
            // we can't send a RetryableHazelcastException explicitly since it would retry this opertation and we want to retry
            // the preceding EntryOffloadableOperation that this operation is part of.
            throw new EntryOffloadableLockMismatchException(
                    String.format("The key is not locked by the caller=%s and threadId=%d", caller, threadId));
        }
    }

    private void unlockKey() {
        boolean unlocked = recordStore.unlock(dataKey, caller, threadId, getCallId());
        if (!unlocked) {
            throw new IllegalStateException(
                    String.format("Unexpected error! EntryOffloadableSetUnlockOperation finished but the unlock method "
                            + "returned false for caller=%s and threadId=%d", caller, threadId));
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        disposeDeferredBlocks();
    }

    @Override
    public boolean returnsResponse() {
        // this has to be true, otherwise the calling side won't be notified about the exception thrown by this operation
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return entryBackupProcessor != null ? new HDEntryBackupOperation(name, dataKey, entryBackupProcessor) : null;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryBackupProcessor != null;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return new LockWaitNotifyKey(new DistributedObjectNamespace(SERVICE_NAME, name), dataKey);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.ENTRY_OFFLOADABLE_SET_UNLOCK;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(modificationType != null ? modificationType.name() : "");
        out.writeData(oldValue);
        out.writeData(newValue);
        out.writeUTF(caller);
        out.writeLong(begin);
        out.writeObject(entryBackupProcessor);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        String modificationTypeName = in.readUTF();
        modificationType = modificationTypeName.equals("") ? null : EntryEventType.valueOf(modificationTypeName);
        oldValue = in.readData();
        newValue = in.readData();
        caller = in.readUTF();
        begin = in.readLong();
        entryBackupProcessor = in.readObject();
    }

}
