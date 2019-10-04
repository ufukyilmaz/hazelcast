package com.hazelcast.internal.hotrestart.backup;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.hotrestart.HotBackupService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.transaction.impl.TargetAwareTransactionLogRecord;
import com.hazelcast.internal.util.Preconditions;

import java.io.IOException;
import java.util.UUID;

/**
 * {@link com.hazelcast.transaction.impl.TransactionLogRecord} implementation for cluster-wide backup of hot restart data.
 *
 * @see HotBackupService
 */
public class HotRestartBackupTransactionLogRecord implements TargetAwareTransactionLogRecord {

    private long backupSeq;
    private Address initiator;
    private Address target;
    private UUID txnId;
    private long leaseTime;

    public HotRestartBackupTransactionLogRecord() {
    }

    public HotRestartBackupTransactionLogRecord(long backupSeq, Address initiator, Address target,
                                                UUID txnId, long leaseTime) {
        Preconditions.checkNotNull(initiator);
        Preconditions.checkNotNull(target);
        Preconditions.checkNotNull(txnId);
        Preconditions.checkPositive(leaseTime, "Lease time should be positive!");

        this.backupSeq = backupSeq;
        this.initiator = initiator;
        this.target = target;
        this.txnId = txnId;
        this.leaseTime = leaseTime;
    }

    @Override
    public Object getKey() {
        return null;
    }

    @Override
    public Operation newPrepareOperation() {
        return HotRestartBackupOperation.prepareOperation(initiator, txnId, leaseTime);
    }

    @Override
    public Operation newCommitOperation() {
        return HotRestartBackupOperation.commitOperation(backupSeq, initiator, txnId);
    }

    @Override
    public Operation newRollbackOperation() {
        return HotRestartBackupOperation.rollbackOperation(initiator, txnId);
    }

    @Override
    public Address getTarget() {
        return target;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(backupSeq);
        out.writeObject(initiator);
        out.writeObject(target);
        UUIDSerializationUtil.writeUUID(out, txnId);
        out.writeLong(leaseTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        backupSeq = in.readLong();
        initiator = in.readObject();
        target = in.readObject();
        txnId = UUIDSerializationUtil.readUUID(in);
        leaseTime = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return HotRestartBackupSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return HotRestartBackupSerializerHook.BACKUP_TRANSACTION_LOG_RECORD;
    }
}
