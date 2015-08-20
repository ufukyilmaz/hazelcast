package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CacheRemoveOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation {

    private Data currentValue;

    public CacheRemoveOperation() {
    }

    public CacheRemoveOperation(String name, Data key, Data currentValue) {
        super(name, key, true);
        this.currentValue = currentValue;
    }

    @Override
    protected void runInternal() throws Exception {
        if (cache != null) {
            if (currentValue == null) {
                response = cache.remove(key, getCallerUuid(), completionId);
            } else {
                response = cache.remove(key, currentValue, getCallerUuid(), completionId);
            }
        } else {
            response = Boolean.FALSE;
        }
    }

    @Override
    public void afterRun() throws Exception {
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
    public int getId() {
        return HiDensityCacheDataSerializerHook.REMOVE;
    }

}
