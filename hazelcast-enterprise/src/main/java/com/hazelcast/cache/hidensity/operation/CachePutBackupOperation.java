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
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CachePutBackupOperation
        extends AbstractKeyBasedHiDensityCacheOperation
        implements BackupOperation, MutableOperation, MutatingOperation {

    private Data value;
    private ExpiryPolicy expiryPolicy;
    private boolean wanOriginated;

    public CachePutBackupOperation() {
    }

    public CachePutBackupOperation(String name, Data key, Data value,
                                   ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
    }

    public CachePutBackupOperation(String name, Data key, Data value,
                                   ExpiryPolicy expiryPolicy, boolean wanOriginated) {
        this(name, key, value, expiryPolicy);
        this.wanOriginated = wanOriginated;
    }

    @Override
    public void runInternal() throws Exception {
        cache.putBackup(key, value, expiryPolicy);
        response = Boolean.TRUE;
    }

    @Override
    public void afterRun() throws Exception {
        if (!wanOriginated && cache.isWanReplicationEnabled()) {
            CacheRecord cacheRecord = cache.getRecord(key);
            CacheEntryView<Data, Data> entryView = CacheEntryViews.createDefaultEntryView(
                    cache.toEventData(key), cache.toEventData(cacheRecord.getValue()), cacheRecord);
            CacheWanEventPublisher publisher = cacheService.getCacheWanEventPublisher();
            publisher.publishWanReplicationUpdateBackup(name, entryView);
        }
        super.afterRun();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        // If run is completed successfully, don't dispose key and value since they are handled in the record store.
        // Although run is completed successfully there may be still error (while sending response, ...), in this case,
        // unused data (such as key on update) is handled (disposed) through `dispose()` > `disposeDeferredBlocks()`.
        if (!runCompleted) {
            serializationService.disposeData(key);
            serializationService.disposeData(value);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
        out.writeData(value);
        out.writeBoolean(wanOriginated);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();
        value = readNativeMemoryOperationData(in);
        wanOriginated = in.readBoolean();
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.PUT_BACKUP;
    }

}
