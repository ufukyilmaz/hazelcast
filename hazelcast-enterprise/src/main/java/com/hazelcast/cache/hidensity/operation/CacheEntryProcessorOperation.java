package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.spi.Operation;

import javax.cache.processor.EntryProcessor;
import java.io.IOException;

/**
 * Operation of the Cache Entry Processor.
 * <p>{@link javax.cache.processor.EntryProcessor} is executed on the partition.
 * {@link com.hazelcast.cache.impl.ICacheRecordStore} provides the required functionality and this
 * operation is responsible for parameter passing and handling the backup at the end.</p>
 */
public class CacheEntryProcessorOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation {

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
    protected void runInternal() throws Exception {
        response = cache.invoke(key, entryProcessor, arguments, completionId);
        CacheRecord record = cache.getRecord(key);
        if (record != null) {
            Object recordVal = record.getValue();
            if (recordVal instanceof Data) {
                Data dataVal = (Data) recordVal;
                if (dataVal instanceof NativeMemoryData) {
                    backupData = serializationService.convertData(dataVal, DataType.HEAP);
                } else {
                    backupData = dataVal;
                }
            }
        }
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        serializationService.disposeData(key);
    }

    @Override
    public Operation getBackupOperation() {
        if (backupData != null) {
            // After entry processor is executed if there is a record, this means that possible add/update
            return new CachePutBackupOperation(name, key, backupData, null);
        } else {
            // If there is no record, this means possible remove by entry processor.
            // TODO In case of non-existing key, this cause redundant remove operation to backups
            // Better solution may be using a new interface like "EntryProcessorListener" on "invoke" method
            // for handling add/update/remove cases properly at execution of "EntryProcessor".
            return new CacheRemoveBackupOperation(name, key);
        }
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
        return HiDensityCacheDataSerializerHook.ENTRY_PROCESSOR;
    }

}
