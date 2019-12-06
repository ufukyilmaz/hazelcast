package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.cache.processor.EntryProcessor;
import java.io.IOException;

import static com.hazelcast.cache.impl.record.CacheRecord.TIME_NOT_AVAILABLE;

/**
 * Operation of the Cache Entry Processor.
 * <p>{@link javax.cache.processor.EntryProcessor} is executed on the partition.
 * {@link com.hazelcast.cache.impl.ICacheRecordStore} provides the required functionality and this
 * operation is responsible for parameter passing and handling the backup at the end.</p>
 */
public class CacheEntryProcessorOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    private EntryProcessor entryProcessor;
    private Object[] arguments;

    private transient Data backupData;
    private transient long backupRecordCreationTime = TIME_NOT_AVAILABLE;

    public CacheEntryProcessorOperation() {
    }

    public CacheEntryProcessorOperation(String name, Data key, int completionId,
                                        EntryProcessor entryProcessor, Object... arguments) {
        super(name, key, completionId);
        this.entryProcessor = entryProcessor;
        this.arguments = arguments;
    }

    @Override
    protected void runInternal() {
        response = recordStore.invoke(key, entryProcessor, arguments, completionId);
        CacheRecord record = recordStore.getRecord(key);
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
            backupRecordCreationTime = record.getCreationTime();
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (recordStore.isWanReplicationEnabled()) {
            if (backupData != null) {
                publishWanUpdate(key, recordStore.getRecord(key));
            } else {
                publishWanRemove(key);
            }
        }
        super.afterRun();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        // if run is completed successfully, don't dispose key since it is handled in the record store
        // although run is completed successfully there may be still error (while sending response, ...), in this case,
        // unused data (such as key on update) is handled (disposed) through `dispose()` > `disposeDeferredBlocks()`
        if (!runCompleted) {
            serializationService.disposeData(key);
        }
    }

    @Override
    public Operation getBackupOperation() {
        if (backupData != null) {
            // after entry processor is executed if there is a record, this means that
            // there is a possible add/update
            return new CachePutBackupOperation(name, key, backupData, null, backupRecordCreationTime);
        } else {
            // if there is no record, this means possible remove by entry processor
            // TODO In case of non-existing key, this cause redundant remove operation to backups
            // better solution may be using a new interface like "EntryProcessorListener" on "invoke" method
            // for handling add/update/remove cases properly at execution of "EntryProcessor"
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
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.ENTRY_PROCESSOR;
    }

}
