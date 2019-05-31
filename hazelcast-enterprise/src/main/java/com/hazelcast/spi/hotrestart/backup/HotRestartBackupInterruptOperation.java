package com.hazelcast.spi.hotrestart.backup;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.hotrestart.HotBackupService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;

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
