package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.hidensity.HiDensityCacheRecord;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.NativeMemoryData;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Cache PutAllBackup Operation is the backup operation used by load all operation. Provides backup of
 * multiple entries.
 *
 * @see com.hazelcast.cache.impl.operation.CacheLoadAllOperation
 */
public class CachePutAllBackupOperation
        extends AbstractNamedOperation
        implements BackupOperation, IdentifiedDataSerializable {

    private Map<Data, CacheRecord> cacheRecords;
    private transient HiDensityCacheRecordStore cache;

    public CachePutAllBackupOperation() {
    }

    public CachePutAllBackupOperation(String name, Map<Data, CacheRecord> cacheRecords) {
        super(name);
        this.cacheRecords = cacheRecords;
    }

    @Override
    public void beforeRun() throws Exception {
        CacheService service = getService();
        cache = (HiDensityCacheRecordStore) service.getOrCreateRecordStore(name, getPartitionId());
    }

    @Override
    public void run() throws Exception {
        if (cacheRecords != null) {
            for (Map.Entry<Data, CacheRecord> entry : cacheRecords.entrySet()) {
                cache.putRecord(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        EnterpriseCacheService service = getService();
        EnterpriseSerializationService serializationService = service.getSerializationService();
        super.writeInternal(out);
        out.writeBoolean(cacheRecords != null);
        if (cacheRecords != null) {
            out.writeInt(cacheRecords.size());
            for (Map.Entry<Data, CacheRecord> entry : cacheRecords.entrySet()) {
                final Data key = entry.getKey();
                final CacheRecord record = entry.getValue();
                if (key instanceof NativeMemoryData) {
                    out.writeData(serializationService.convertData(key, DataType.HEAP));
                } else {
                    out.writeData(key);
                }
                if (record instanceof HiDensityCacheRecord) {
                    out.writeBoolean(true);
                    writeHiDensityCacheRecord(out, serializationService,
                            (HiDensityCacheRecord) record);
                } else {
                    out.writeBoolean(false);
                    out.writeObject(record);
                }
            }
        }
    }

    private void writeHiDensityCacheRecord(ObjectDataOutput out,
                                           EnterpriseSerializationService serializationService,
                                           HiDensityCacheRecord record) throws IOException {
        if (record == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);

        out.writeLong(record.getCreationTime());
        out.writeInt(record.getAccessTimeDiff());
        out.writeInt(record.getAccessHit());
        out.writeInt(record.getTtlMillis());

        Data valueData = (Data) record.getValue();
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
        out.writeObject(record);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        EnterpriseCacheService service = getService();
        EnterpriseSerializationService serializationService = service.getSerializationService();
        super.readInternal(in);
        final boolean recordNotNull = in.readBoolean();
        if (recordNotNull) {
            int size = in.readInt();
            cacheRecords = new HashMap<Data, CacheRecord>(size);
            for (int i = 0; i < size; i++) {
                final Data key = in.readData();
                CacheRecord record;
                final boolean isHiDensityCacheRecord = in.readBoolean();
                if (isHiDensityCacheRecord) {
                    record = readHiDensityCacheRecord(in, serializationService);
                } else {
                    record = in.readObject();
                }
                cacheRecords.put(key, record);
            }
        }
    }

    private HiDensityCacheRecord readHiDensityCacheRecord(
            ObjectDataInput in, EnterpriseSerializationService serializationService) throws IOException {
        HiDensityCacheRecord record = null;
        NativeMemoryData nativeMemoryData = null;
        try {
            final boolean recordNotNull = in.readBoolean();
            if (recordNotNull) {
                record = (HiDensityCacheRecord) cache.getRecordProcessor().newRecord();

                record.setCreationTime(in.readLong());
                record.setAccessTimeDiff(in.readInt());
                record.setAccessHit(in.readInt());
                record.setTtlMillis(in.readInt());

                final boolean valueNotNull = in.readBoolean();
                if (valueNotNull) {
                    Data valueData = in.readData();
                    nativeMemoryData = (NativeMemoryData) cache.getRecordProcessor().convertData(valueData, DataType.NATIVE);
                    record.setValue(nativeMemoryData);
                } else {
                    record.setValue(null);
                }
                return record;
            } else {
                return null;
            }
        } catch (Throwable t) {
            if (record != null && record.address() != MemoryManager.NULL_ADDRESS) {
                cache.getRecordProcessor().dispose(record);
            }
            if (nativeMemoryData != null && nativeMemoryData.address() != MemoryManager.NULL_ADDRESS) {
                cache.getRecordProcessor().disposeData(nativeMemoryData);
            }
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.PUT_ALL_BACKUP;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }
}
