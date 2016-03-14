package com.hazelcast.map.impl.operation;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

/**
 * Flushes dirty entries upon call of {@link IMap#flush()}
 */
public class HDMapFlushOperation extends HDMapOperation implements BackupAwareOperation, MutatingOperation {

    private transient long sequence;

    public HDMapFlushOperation() {
    }

    public HDMapFlushOperation(String name) {
        super(name);
    }

    @Override
    protected void runInternal() {
        sequence = recordStore.softFlush();
    }

    @Override
    public Object getResponse() {
        return sequence;
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
