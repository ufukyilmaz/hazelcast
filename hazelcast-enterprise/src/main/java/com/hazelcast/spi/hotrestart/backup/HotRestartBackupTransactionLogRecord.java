package com.hazelcast.spi.hotrestart.backup;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.HotBackupService;
import com.hazelcast.transaction.impl.TargetAwareTransactionLogRecord;
import com.hazelcast.util.Preconditions;

import java.io.IOException;

/**
 * {@link com.hazelcast.transaction.impl.TransactionLogRecord} implementation for cluster-wide backup of hot restart data.
 *
 * @see HotBackupService
 */
public class HotRestartBackupTransactionLogRecord implements TargetAwareTransactionLogRecord {

    private long backupSeq;
    private Address initiator;
    private Address target;
    private String txnId;
    private long leaseTime;

    public HotRestartBackupTransactionLogRecord() {
    }

    public HotRestartBackupTransactionLogRecord(long backupSeq, Address initiator, Address target,
                                                String txnId, long leaseTime) {
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
        initiator.writeData(out);
        target.writeData(out);
        out.writeUTF(txnId);
        out.writeLong(leaseTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        backupSeq = in.readLong();
        initiator = new Address();
        initiator.readData(in);
        target = new Address();
        target.readData(in);
        txnId = in.readUTF();
        leaseTime = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return HotRestartBackupSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HotRestartBackupSerializerHook.BACKUP_TRANSACTION_LOG_RECORD;
    }
}
