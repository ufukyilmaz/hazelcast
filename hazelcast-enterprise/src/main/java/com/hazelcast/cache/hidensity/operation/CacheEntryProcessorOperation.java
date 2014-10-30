package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.OffHeapData;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Operation;

import javax.cache.processor.EntryProcessor;
import java.io.IOException;

/**
 * Operation of the Cache Entry Processor.
 * <p>{@link javax.cache.processor.EntryProcessor} is executed on the partition.
 * {@link com.hazelcast.cache.impl.ICacheRecordStore} provides the required functionality and this
 * operation is responsible for parameter passing and handling the backup at the end.</p>
 */
public class CacheEntryProcessorOperation extends BackupAwareHiDensityCacheOperation {

    private EntryProcessor entryProcessor;
    private Object[] arguments;

    private transient Data backupData;

    public CacheEntryProcessorOperation() {
    }

    public CacheEntryProcessorOperation(String name, Data key, int completionId,
                                        EntryProcessor entryProcessor, Object... arguments) {
        super(name, key, completionId);
        this.entryProcessor = entryProcessor;
        this.arguments = arguments;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, backupData, null);
    }

    @Override
    public void runInternal() throws Exception {
        EnterpriseCacheService service = getService();
        EnterpriseSerializationService serializationService = service.getSerializationService();
        response = cache.invoke(key, entryProcessor, arguments);
        CacheRecord record = cache.getRecord(key);
        if (record != null) {
            Object recordVal = record.getValue();
            if (recordVal instanceof Data) {
                Data dataVal = (Data) recordVal;
                if (dataVal instanceof OffHeapData) {
                    backupData = serializationService.convertData(dataVal, DataType.HEAP);
                } else {
                    backupData = dataVal;
                }
            }
        }
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
        out.writeBoolean(arguments != null);
        if (arguments != null) {
            out.writeInt(arguments.length);
            for (Object arg : arguments) {
                out.writeObject(arg);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
        final boolean hasArguments = in.readBoolean();
        if (hasArguments) {
            final int size = in.readInt();
            arguments = new Object[size];
            for (int i = 0; i < size; i++) {
                arguments[i] = in.readObject();
            }
        }
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.ENTRY_PROCESSOR;
    }
}
