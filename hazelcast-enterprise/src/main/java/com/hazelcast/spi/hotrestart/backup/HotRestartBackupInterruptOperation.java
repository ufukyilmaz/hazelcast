package com.hazelcast.spi.hotrestart.backup;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.hotrestart.HotBackupService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.transaction.TransactionException;

/**
 * Operation for interruption of the member hot restart backup task.
 */
public class HotRestartBackupInterruptOperation extends Operation implements AllowedDuringPassiveState,
        IdentifiedDataSerializable {
    public HotRestartBackupInterruptOperation() {
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        final HotBackupService service = getService();
        if (service != null) {
            service.interruptLocalBackupTask();
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
    public int getFactoryId() {
        return HotRestartBackupSerializerHook.F_ID;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public int getId() {
        return HotRestartBackupSerializerHook.BACKUP_INTERRUPT;
    }
}
