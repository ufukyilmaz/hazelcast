package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Executes the putAll() operation on HD caches.
 */
public class CachePutAllOperation
        extends BackupAwareHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    private List<Map.Entry<Data, Data>> entries;
    private ExpiryPolicy expiryPolicy;

    private transient CacheBackupRecordStore cacheBackupRecordStore;

    public CachePutAllOperation() {
    }

    public CachePutAllOperation(String name, List<Map.Entry<Data, Data>> entries, ExpiryPolicy expiryPolicy) {
        super(name);
        this.entries = entries;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void beforeRunInternal() {
        super.beforeRunInternal();

        int backups = getSyncBackupCount() + getAsyncBackupCount();
        if (backups > 0) {
            cacheBackupRecordStore = new CacheBackupRecordStore(entries.size());
        }
    }

    @Override
    protected void runInternal() {
        String callerUuid = getCallerUuid();

        Iterator<Map.Entry<Data, Data>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<Data, Data> entry = iterator.next();
            Data key = entry.getKey();
            Data value = entry.getValue();
            final CacheRecord record = recordStore.put(key, value, expiryPolicy, callerUuid, completionId);

            /*
             * We should be sure that backup and WAN event records are heap based.
             * Because keys/values, have been already put to record store,
             * might be evicted inside the loop while trying to put others.
             * So in this case, internal backupRecords map or WAN event contains
             * invalid (disposed) keys and records and this is passed to
             * CachePutAllBackupOperation. Then possibly there will be JVM crash or
             * serialization exception.
             */
            Data onHeapKey = null;
            Data onHeapValue = null;
            if (cacheBackupRecordStore != null && record != null) {
                onHeapKey = serializationService.convertData(key, DataType.HEAP);
                onHeapValue = serializationService.convertData(value, DataType.HEAP);

                cacheBackupRecordStore.addBackupRecord(onHeapKey, onHeapValue, record.getCreationTime());
            }

            if (onHeapValue != null) {
                publishWanUpdate(onHeapKey, onHeapValue, record);
            } else {
                publishWanUpdate(onHeapKey, record);
            }

            // we remove each fully processed entry, so it won't be processed
            // again on a continuation of the operation after a NativeOOME
            iterator.remove();
        }
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        Iterator<Map.Entry<Data, Data>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<Data, Data> entry = iterator.next();
            Data key = entry.getKey();
            Data value = entry.getValue();
            serializationService.disposeData(key);
            serializationService.disposeData(value);
            iterator.remove();
        }
    }

    @Override
    public boolean shouldBackup() {
        return cacheBackupRecordStore != null && !cacheBackupRecordStore.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, cacheBackupRecordStore, expiryPolicy);
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.PUT_ALL;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
        out.writeInt(entries.size());
        for (Map.Entry<Data, Data> entry : entries) {
            out.writeData(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();
        int size = in.readInt();
        entries = new ArrayList<Map.Entry<Data, Data>>(size);
        for (int i = 0; i < size; i++) {
            Data key = readNativeMemoryOperationData(in);
            Data value = readNativeMemoryOperationData(in);
            entries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value));
        }
    }
}
