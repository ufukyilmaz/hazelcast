package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.hidensity.HiDensityCacheRecord;
import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheDataRecord;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.EnterpriseObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Cache PutAllBackup Operation is the backup operation used by load all operation. Provides backup of
 * multiple entries.
 *
 * @see com.hazelcast.cache.impl.operation.CacheLoadAllOperation
 */
public class CachePutAllBackupOperation
        extends AbstractHiDensityCacheOperation
        implements BackupOperation, MutableOperation, MutatingOperation {

    private Map<Data, CacheRecord> cacheRecords;

    public CachePutAllBackupOperation() {
    }

    public CachePutAllBackupOperation(String name, Map<Data, CacheRecord> cacheRecords) {
        super(name);
        this.cacheRecords = cacheRecords;
    }

    @Override
    protected void runInternal() throws Exception {
        if (cacheRecords != null) {
            final Iterator<Map.Entry<Data, CacheRecord>> iter = cacheRecords.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Data, CacheRecord> entry = iter.next();
                Data key = entry.getKey();
                CacheRecord record = entry.getValue();
                cache.putRecord(key, record);
                iter.remove();
            }
        }
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        if (cacheRecords != null) {
            final MemoryManager memoryManager = serializationService.getMemoryManager();
            final Iterator<Map.Entry<Data, CacheRecord>> iter = cacheRecords.entrySet().iterator();
            // Dispose remaining entries
            while (iter.hasNext()) {
                Map.Entry<Data, CacheRecord> entry = iter.next();
                Data key = entry.getKey();
                CacheRecord record = entry.getValue();
                serializationService.disposeData(key);
                if (record instanceof HiDensityCacheRecord) {
                    HiDensityCacheRecord hdRecord = (HiDensityCacheRecord) record;
                    NativeMemoryData hdRecordValue = ((HiDensityCacheRecord) record).getValue();
                    long recordAddress = hdRecord.address();
                    if (recordAddress != MemoryManager.NULL_ADDRESS) {
                        memoryManager.free(recordAddress, hdRecord.size());
                    }
                    if (hdRecordValue != null && hdRecordValue.address() != MemoryManager.NULL_ADDRESS) {
                        serializationService.disposeData(hdRecordValue);
                    }
                }
                iter.remove();
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        assert (out instanceof EnterpriseObjectDataOutput)
                : "\"ObjectDataOutput\" must be an \"EnterpriseObjectDataOutput\"";
        final EnterpriseSerializationService serializationService =
                ((EnterpriseObjectDataOutput) out).getSerializationService();

        super.writeInternal(out);

        out.writeBoolean(cacheRecords != null);
        if (cacheRecords != null) {
            out.writeInt(cacheRecords.size());
            for (Map.Entry<Data, CacheRecord> entry : cacheRecords.entrySet()) {
                Data key = entry.getKey();
                CacheRecord record = entry.getValue();
                if (key instanceof NativeMemoryData) {
                    out.writeData(serializationService.convertData(key, DataType.HEAP));
                } else {
                    out.writeData(key);
                }
                if (record instanceof HiDensityCacheRecord) {
                    out.writeBoolean(true);
                    writeCacheRecord(out, (HiDensityCacheRecord) record, serializationService);
                } else {
                    out.writeBoolean(false);
                    out.writeObject(record);
                }
            }
        }
    }

    private void writeCacheRecord(ObjectDataOutput out, HiDensityCacheRecord record,
                                  EnterpriseSerializationService serializationService) throws IOException {
        if (record == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);

        out.writeLong(record.getCreationTime());
        out.writeInt(record.getAccessTimeDiff());
        out.writeInt(record.getAccessHit());
        out.writeInt(record.getTtlMillis());

        Data valueData = record.getValue();
        if (valueData == null) {
            out.writeBoolean(false);
        } else {
            if (valueData instanceof NativeMemoryData) {
                NativeMemoryData nativeMemoryData = (NativeMemoryData) valueData;
                if (nativeMemoryData.address() == MemoryManager.NULL_ADDRESS) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    out.writeData(serializationService.convertData(nativeMemoryData, DataType.HEAP));
                }
            } else {
                out.writeBoolean(true);
                out.writeData(valueData);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        assert (in instanceof EnterpriseObjectDataInput)
                : "\"ObjectDataInput\" must be an \"EnterpriseObjectDataInput\"";
        final EnterpriseSerializationService serializationService =
                ((EnterpriseObjectDataInput) in).getSerializationService();

        super.readInternal(in);

        final boolean recordNotNull = in.readBoolean();
        if (recordNotNull) {
            int size = in.readInt();
            cacheRecords = new HashMap<Data, CacheRecord>(size);
            for (int i = 0; i < size; i++) {
                Data key = AbstractHiDensityCacheOperation.readNativeMemoryOperationData(in);
                CacheRecord record;
                try {
                    final boolean isHiDensityCacheRecord = in.readBoolean();
                    if (isHiDensityCacheRecord) {
                        record = readCacheRecord(in, serializationService);
                    } else {
                        record = in.readObject();
                    }
                    cacheRecords.put(key, record);
                } catch (Throwable t) {
                    serializationService.disposeData(key);
                    disposeInternal(serializationService);
                    throw ExceptionUtil.rethrow(t);
                }
            }
        }
    }

    //CHECKSTYLE:OFF
    private CacheRecord readCacheRecord(ObjectDataInput in, EnterpriseSerializationService serializationService)
            throws IOException {
        final MemoryManager memoryManager = serializationService.getMemoryManager();
        final long recordSize = HiDensityNativeMemoryCacheRecord.SIZE;
        long recordAddress = MemoryManager.NULL_ADDRESS;
        NativeMemoryData nativeMemoryData = null;

        final boolean recordNotNull = in.readBoolean();
        final long creationTime = in.readLong();
        final int accessTimeDiff = in.readInt();
        final int accessHit = in.readInt();
        final int ttlMillis = in.readInt();
        final boolean valueNotNull = in.readBoolean();
        Data valueData = valueNotNull ? AbstractHiDensityCacheOperation.readNativeMemoryOperationData(in) : null;
        if (valueData instanceof NativeMemoryData) {
            nativeMemoryData = (NativeMemoryData) valueData;
        }
        try {
            if (recordNotNull) {
                recordAddress = memoryManager.allocate(recordSize);
                HiDensityCacheRecord cacheRecord = new HiDensityNativeMemoryCacheRecord(recordAddress);
                cacheRecord.setCreationTime(creationTime);
                cacheRecord.setAccessTimeDiff(accessTimeDiff);
                cacheRecord.setAccessHit(accessHit);
                cacheRecord.setTtlMillis(ttlMillis);
                if (valueNotNull) {
                    if (nativeMemoryData == null) {
                        nativeMemoryData = serializationService.convertData(valueData, DataType.NATIVE);
                    }
                    cacheRecord.setValue(nativeMemoryData);
                } else {
                    cacheRecord.setValue(null);
                }
                return cacheRecord;
            } else {
                return null;
            }
        } catch (Throwable t) {
            final boolean readToHeap = t instanceof NativeOutOfMemoryError;
            if (recordAddress != MemoryManager.NULL_ADDRESS) {
                memoryManager.free(recordAddress, recordSize);
            }
            if (nativeMemoryData != null && nativeMemoryData.address() != MemoryManager.NULL_ADDRESS) {
                if (readToHeap) {
                    valueData = serializationService.convertData(nativeMemoryData, DataType.HEAP);
                }
                serializationService.disposeData(nativeMemoryData);
            }
            if (readToHeap) {
                // Read record to heap
                CacheDataRecord cacheRecord = new CacheDataRecord();
                cacheRecord.setCreationTime(creationTime);
                cacheRecord.setAccessHit(accessHit);
                cacheRecord.setAccessTime(creationTime + accessTimeDiff);
                if (ttlMillis >= 0) {
                    cacheRecord.setExpirationTime(creationTime + ttlMillis);
                }
                if (valueNotNull) {
                   cacheRecord.setValue(valueData);
                } else {
                   cacheRecord.setValue(null);
                }
                return cacheRecord;
            } else {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }
    //CHECKSTYLE:ON

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.PUT_ALL_BACKUP;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

}
