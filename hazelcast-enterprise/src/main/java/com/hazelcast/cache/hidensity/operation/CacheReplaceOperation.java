package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.CacheEntryViews;
import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Replaces the value of a JCache entry.
 */
public class CacheReplaceOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    private Data value;
    private Data currentValue;
    private ExpiryPolicy expiryPolicy;

    public CacheReplaceOperation() {
    }

    public CacheReplaceOperation(String name, Data key, Data oldValue,
                                 Data newValue, ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.value = newValue;
        this.currentValue = oldValue;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void runInternal() throws Exception {
        if (currentValue == null) {
            response = cache.replace(key, value, getCallerUuid(), completionId);
        } else {
            response = cache.replace(key, currentValue, value, getCallerUuid(), completionId);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            if (cache.isWanReplicationEnabled()) {
                CacheRecord cacheRecord = cache.getRecord(key);
                CacheEntryView<Data, Data> entryView = CacheEntryViews.createDefaultEntryView(cache.toEventData(key),
                        cache.toEventData(cacheRecord.getValue()), cacheRecord);
                CacheWanEventPublisher publisher = cacheService.getCacheWanEventPublisher();
                publisher.publishWanReplicationUpdate(name, entryView);
            }
        }
        super.afterRun();
        dispose();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        serializationService.disposeData(key);
        if (!Boolean.TRUE.equals(response)) {
            serializationService.disposeData(value);
        }
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
        return new CachePutBackupOperation(name, key, value, expiryPolicy);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(value);
        out.writeData(currentValue);
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = readNativeMemoryOperationData(in);
        currentValue = readNativeMemoryOperationData(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.REPLACE;
    }

}
