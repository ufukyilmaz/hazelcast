package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Removes the mapping for a key from this cache if it is present.
 */
public class CacheRemoveOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    private Data currentValue;

    public CacheRemoveOperation() {
    }

    public CacheRemoveOperation(String name, Data key, Data currentValue) {
        super(name, key);
        this.currentValue = currentValue;
    }

    @Override
    protected void runInternal() {
        if (currentValue == null) {
            response = recordStore.remove(key, getCallerUuid(), null, completionId);
        } else {
            response = recordStore.remove(key, currentValue, getCallerUuid(), null, completionId);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            publishWanRemove(key);
        }
        super.afterRun();
        dispose();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        serializationService.disposeData(key);
        if (currentValue != null) {
            serializationService.disposeData(currentValue);
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheRemoveBackupOperation(name, key);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(currentValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        currentValue = readNativeMemoryOperationData(in);
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.REMOVE;
    }
}
