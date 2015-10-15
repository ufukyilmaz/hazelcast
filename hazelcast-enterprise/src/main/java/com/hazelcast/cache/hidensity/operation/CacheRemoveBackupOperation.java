package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CacheRemoveBackupOperation
        extends AbstractKeyBasedHiDensityCacheOperation
        implements BackupOperation, MutableOperation, MutatingOperation {

    private boolean wanOriginated;

    public CacheRemoveBackupOperation() {
    }

    public CacheRemoveBackupOperation(String name, Data key) {
        super(name, key, true);
    }

    public CacheRemoveBackupOperation(String name, Data key, boolean wanOriginated) {
        this(name, key);
        this.wanOriginated = wanOriginated;
    }

    @Override
    public void runInternal() throws Exception {
        if (cache != null) {
            response = cache.removeRecord(key);
        } else {
            response = Boolean.FALSE;
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response) && !wanOriginated) {
            if (cache.isWanReplicationEnabled()) {
                CacheWanEventPublisher publisher = cacheService.getCacheWanEventPublisher();
                publisher.publishWanReplicationRemoveBackup(name, cache.toEventData(key));
            }
        }
        super.afterRun();
        dispose();
    }


    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        serializationService.disposeData(key);
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.REMOVE_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(wanOriginated);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        wanOriginated = in.readBoolean();
    }
}
