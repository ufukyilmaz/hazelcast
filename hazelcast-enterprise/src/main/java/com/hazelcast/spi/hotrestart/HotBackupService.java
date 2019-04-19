package com.hazelcast.spi.hotrestart;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.hotrestart.BackupTaskStatus;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.util.LockGuard;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.hotrestart.backup.HotRestartBackupInterruptOperation;
import com.hazelcast.spi.hotrestart.backup.HotRestartBackupTransactionLogRecord;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Service for initiating a cluster-wide backup of hot restart data.
 */
public class HotBackupService implements HotRestartService, TransactionalService {
    /** Name of the Hot Restart backup service. */
    public static final String SERVICE_NAME = "hz:ee:internalHotBackupService";

    private static final TransactionOptions DEFAULT_TX_OPTIONS = new TransactionOptions()
            .setDurability(1)
            .setTimeout(1, TimeUnit.MINUTES)
            .setTransactionType(TransactionType.TWO_PHASE);
    private static final long LOCK_LEASE_EXTENSION_MILLIS = TimeUnit.SECONDS.toMillis(20);
    private final Node node;
    private final Lock serviceLock = new ReentrantLock();
    private final AtomicReference<LockGuard> lockGuardRef = new AtomicReference<>(LockGuard.NOT_LOCKED);
    private final HotRestartIntegrationService hotRestartService;

    public HotBackupService(Node node, HotRestartIntegrationService hotRestartService) {
        this.node = node;
        this.hotRestartService = hotRestartService;
    }

    @Override
    public void backup() {
        backup(node.getClusterService().getClusterClock().getClusterTime());
    }

    @Override
    public void backup(long backupSeq) {
        final NodeEngineImpl nodeEngine = node.getNodeEngine();
        final TransactionManagerServiceImpl txManagerService
                = (TransactionManagerServiceImpl) nodeEngine.getTransactionManagerService();
        final Transaction tx = txManagerService.newAllowedDuringPassiveStateTransaction(DEFAULT_TX_OPTIONS);
        tx.begin();

        try {
            addTransactionRecords(backupSeq, tx, node.getClusterService().getMembers());
            tx.prepare();
        } catch (Throwable e) {
            tx.rollback();
            throw rethrow(e);
        }

        try {
            tx.commit();
        } catch (Throwable e) {
            if (e instanceof TargetNotMemberException || e.getCause() instanceof MemberLeftException) {
                // Member left while tx is being committed after prepare successful.
                // We cannot rollback tx after this point. Cluster state change is done
                // on other members.
                return;
            }
            throw rethrow(e);
        }
    }

    @Override
    public BackupTaskStatus getBackupTaskStatus() {
        return hotRestartService.getBackupTaskStatus();
    }

    @Override
    public void interruptLocalBackupTask() {
        hotRestartService.interruptBackupTask();
    }

    @Override
    public void interruptBackupTask() {
        broadcast(new HotRestartBackupInterruptOperation());
        interruptLocalBackupTask();
    }

    @Override
    public boolean isHotBackupEnabled() {
        return true;
    }

    @Override
    public String getBackupDirectory() {
        return node.getConfig().getHotRestartPersistenceConfig().getBackupDir().getAbsolutePath();
    }

    private void broadcast(Operation operation) {
        final InternalOperationService operationService = node.getNodeEngine().getOperationService();
        for (Member member : node.getClusterService().getMembers()) {
            final Address target = member.getAddress();
            if (!node.getThisAddress().equals(target)) {
                operationService.send(operation, target);
            }
        }
    }

    private void addTransactionRecords(long backupSeq, Transaction tx, Collection<Member> members) {
        final long leaseTime = Math.min(tx.getTimeoutMillis(), LOCK_LEASE_EXTENSION_MILLIS);
        for (Member member : members) {
            tx.add(new HotRestartBackupTransactionLogRecord(backupSeq, node.getThisAddress(), member.getAddress(),
                    tx.getTxnId(), leaseTime));
        }
    }

    /**
     * Attempt to prepare for a hot restart backup on this node. This method is intended to be called inside a transaction
     * created by {@link #backup()} or {@link #backup(long)}.
     *
     * @param initiator the member which initiated the transaction
     * @param txnId     the transaction ID
     * @param leaseTime number of ms after which this transaction will expire
     */
    public void prepareBackup(Address initiator, String txnId, long leaseTime) {
        serviceLock.lock();
        try {
            if (hotRestartService.isBackupInProgress()) {
                throw new IllegalStateException("Another backup is currently in progress, aborting backup request");
            }
            final LockGuard backupGuard = getBackupGuard();
            checkForExistingTransaction(backupGuard.allowsLock(txnId), initiator, backupGuard);

            lockGuardRef.set(new LockGuard(initiator, txnId, leaseTime));
        } finally {
            serviceLock.unlock();
        }
    }

    /**
     * Attempt to perform the hot restart backup on this node. This method is intended to be called inside a transaction
     * created by {@link #backup()} or {@link #backup(long)}.
     *
     * @param backupSeq the suffix for the hot restart backup directory
     * @param initiator the member which initated the transaction
     * @param txnId     the transaction ID
     */
    public void commitBackup(long backupSeq, Address initiator, String txnId) {
        serviceLock.lock();
        try {
            if (hotRestartService.isBackupInProgress()) {
                lockGuardRef.set(LockGuard.NOT_LOCKED);
                throw new IllegalStateException("Another backup is currently in progress, aborting backup request");
            }
            final LockGuard backupGuard = getBackupGuard();
            checkForExistingTransaction(backupGuard.allowsUnlock(txnId), initiator, backupGuard);

            hotRestartService.backup(backupSeq);
            lockGuardRef.set(LockGuard.NOT_LOCKED);
        } finally {
            serviceLock.unlock();
        }
    }

    /**
     * Rollback the transaction for the hot restart backup.
     *
     * @param txnId the transaction ID
     * @return if the transaction was rolled back
     */
    public boolean rollbackBackup(String txnId) {
        serviceLock.lock();
        try {
            final LockGuard backupGuard = getBackupGuard();
            if (!backupGuard.allowsUnlock(txnId)) {
                return false;
            }
            lockGuardRef.set(LockGuard.NOT_LOCKED);
            return true;
        } finally {
            serviceLock.unlock();
        }
    }

    private void checkForExistingTransaction(boolean check, Address initiator, LockGuard backupGuard) {
        if (!check) {
            throw new TransactionException(
                    "Hot restart backup failed for " + initiator + " because a backup request from "
                            + backupGuard.getLockOwner() + " is in progress");
        }
    }

    private LockGuard getBackupGuard() {
        LockGuard lockGuard = lockGuardRef.get();
        while (lockGuard.isLeaseExpired()) {
            if (lockGuardRef.compareAndSet(lockGuard, LockGuard.NOT_LOCKED)) {
                lockGuard = LockGuard.NOT_LOCKED;
                break;
            }
            lockGuard = lockGuardRef.get();
        }
        return lockGuard;
    }

    @Override
    public <T extends TransactionalObject> T createTransactionalObject(String name, Transaction transaction) {
        throw new UnsupportedOperationException(SERVICE_NAME + " does not support TransactionalObjects!");
    }

    @Override
    public void rollbackTransaction(String transactionId) {
        rollbackBackup(transactionId);
    }
}
