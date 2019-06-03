package com.hazelcast.spi.hotrestart.backup;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.hotrestart.HotBackupService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.EmptyStatement;

import java.io.IOException;

import static com.hazelcast.transaction.impl.Transaction.State.COMMITTING;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARING;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLING_BACK;

/**
 * Operation for performing cluster hot restart data backup. The backup operation is performed inside a transaction and this
 * class is used for all phases of the transaction. The {@link #prepareOperation(Address, String, long)} method is used for the
 * prepare phase, the {@link #commitOperation(long, Address, String)} is used for the commit phase and the
 * {@link #rollbackOperation(Address, String)} is used for for creating a new operation for the rollback phase.
 */
public class HotRestartBackupOperation extends Operation implements AllowedDuringPassiveState, IdentifiedDataSerializable {
    private String transactionPhaseName;
    private Transaction.State transactionPhase;
    private long backupSeq;
    private Address initiator;
    private String txnId;
    private long leaseTime;

    public HotRestartBackupOperation() {
    }

    private HotRestartBackupOperation(Transaction.State transactionPhase, long backupSeq, Address initiator, String txnId,
                                      long leaseTime) {
        this.transactionPhase = transactionPhase;
        this.backupSeq = backupSeq;
        this.initiator = initiator;
        this.txnId = txnId;
        this.leaseTime = leaseTime;
    }

    /**
     * Create a operation for preparing a transaction of a cluster-wide hot restart backup.
     *
     * @param initiator the member which initiated the backup
     * @param txnId     the transaction ID
     * @param leaseTime duration in ms after which this transaction will expire
     * @return the prepare operation for hot restart backup
     */
    public static HotRestartBackupOperation prepareOperation(Address initiator, String txnId, long leaseTime) {
        return new HotRestartBackupOperation(PREPARING, 0, initiator, txnId, leaseTime);
    }

    /**
     * Create a operation for commiting a transaction of a cluster-wide hot restart backup.
     *
     * @param backupSeq the suffix for the hot restart backup directory
     * @param initiator the member which initiated the backup
     * @param txnId     the transaction ID
     * @return the commit operation for hot restart backup
     */
    public static HotRestartBackupOperation commitOperation(long backupSeq, Address initiator, String txnId) {
        return new HotRestartBackupOperation(COMMITTING, backupSeq, initiator, txnId, 0);
    }

    /**
     * Create a operation for transaction rollback of a cluster-wide hot restart backup.
     *
     * @param initiator the member which initiated the backup
     * @param txnId     the transaction ID
     * @return the rollback operation for hot restart backup
     */
    public static HotRestartBackupOperation rollbackOperation(Address initiator, String txnId) {
        return new HotRestartBackupOperation(ROLLING_BACK, 0, initiator, txnId, 0);
    }

    @Override
    public void beforeRun() throws Exception {
        if (transactionPhase == null
                || (transactionPhase != PREPARING && transactionPhase != COMMITTING && transactionPhase != ROLLING_BACK)) {
            throw new IllegalArgumentException("Transaction phase is not allowed: " + transactionPhaseName);
        }
    }

    @Override
    public void run() throws Exception {
        final HotBackupService service = getService();
        switch (transactionPhase) {
            case PREPARING:
                service.prepareBackup(initiator, txnId, leaseTime);
                break;
            case COMMITTING:
                service.commitBackup(backupSeq, initiator, txnId);
                break;
            case ROLLING_BACK:
                getLogger().info("Rolling back cluster state! Initiator: " + initiator);
                service.rollbackBackup(txnId);
                break;
            default:
                throw new IllegalArgumentException("Transaction phase is not allowed: " + transactionPhase);
        }
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof TransactionException) {
            getLogger().severe(e.getMessage());
        } else {
            super.logError(e);
        }
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public String getServiceName() {
        return HotBackupService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(transactionPhase.toString());
        out.writeLong(backupSeq);
        out.writeObject(initiator);
        out.writeUTF(txnId);
        out.writeLong(leaseTime);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionPhaseName = in.readUTF();
        try {
            transactionPhase = Transaction.State.valueOf(transactionPhaseName);
        } catch (IllegalArgumentException ignored) {
            EmptyStatement.ignore(ignored);
        }
        backupSeq = in.readLong();
        initiator = in.readObject();
        txnId = in.readUTF();
        leaseTime = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return HotRestartBackupSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return HotRestartBackupSerializerHook.BACKUP;
    }
}
