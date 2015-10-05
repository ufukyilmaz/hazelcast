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
 * @author mdogan 05/02/14
 */
public class CacheGetAndReplaceOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    private Data value;
    private ExpiryPolicy expiryPolicy;

    public CacheGetAndReplaceOperation() {
    }

    public CacheGetAndReplaceOperation(String name, Data key, Data value,
                                       ExpiryPolicy expiryPolicy) {
        super(name, key, true);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void runInternal() throws Exception {
        response = cache != null ? cache.getAndReplace(key, value, expiryPolicy, getCallerUuid(), completionId) : null;
    }

    @Override
    public void afterRun() throws Exception {
        if (response != null) {
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
        if (response == null) {
            serializationService.disposeData(value);
        }
    }

    @Override
    public boolean shouldBackup() {
        return response != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, value, expiryPolicy);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(value);
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = readNativeMemoryOperationData(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.GET_AND_REPLACE;
    }

}
