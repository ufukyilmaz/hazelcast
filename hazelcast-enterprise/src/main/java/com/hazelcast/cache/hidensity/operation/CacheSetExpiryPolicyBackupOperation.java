package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Backup operation for {@link CacheSetExpiryPolicyOperation}
 */
public class CacheSetExpiryPolicyBackupOperation extends HiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    private transient boolean atLeastOneSucceeded;

    private List<Data> keys;
    private Data expiryPolicy;

    public CacheSetExpiryPolicyBackupOperation() {

    }

    public CacheSetExpiryPolicyBackupOperation(String name, List<Data> keys, Data expiryPolicy) {
        super(name);
        this.keys = keys;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void runInternal() {
        if (recordStore == null) {
            return;
        }
        recordStore.setExpiryPolicy(keys, expiryPolicy, getCallerUuid());
        atLeastOneSucceeded = true;
    }

    @Override
    public void afterRun() throws Exception {
        if (recordStore.isWanReplicationEnabled()) {
            for (Data key : keys) {
                CacheRecord record = recordStore.getRecord(key);
                publishWanUpdate(key, record);
            }
        }
        super.afterRun();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        super.disposeInternal(serializationService);
        if (!atLeastOneSucceeded) {
            serializationService.disposeData(expiryPolicy);
        }
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.SET_EXPIRY_POLICY_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(keys.size());
        for (Data key: keys) {
            out.writeData(key);
        }
        out.writeData(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int s = in.readInt();
        keys = new ArrayList<Data>(s);
        while (s-- > 0) {
            keys.add(in.readData());
        }
        expiryPolicy = in.readData();
    }
}
