package com.hazelcast.map.impl.operation;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDMapFlushOperation extends HDMapOperation implements BackupAwareOperation, MutatingOperation {

    public HDMapFlushOperation() {
    }

    public HDMapFlushOperation(String name) {
        super(name);
    }

    @Override
    protected void runInternal() {
        recordStore.softFlush();
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        MapStoreConfig mapStoreConfig = mapContainer.getMapConfig().getMapStoreConfig();
        return mapStoreConfig != null
                && mapStoreConfig.isEnabled()
                && mapStoreConfig.getWriteDelaySeconds() > 0;
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
    public Operation getBackupOperation() {
        return new HDMapFlushBackupOperation(name);
    }

}
