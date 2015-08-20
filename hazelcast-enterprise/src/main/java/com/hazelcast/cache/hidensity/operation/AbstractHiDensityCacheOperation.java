package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.logging.Level;

/**
 * @author sozal 07/08/15
 */
abstract class AbstractHiDensityCacheOperation
        extends AbstractOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected static final int FORCED_EVICTION_RETRY_COUNT =
            ICacheRecordStore.ONE_HUNDRED_PERCENT / HiDensityCacheRecordStore.DEFAULT_FORCED_EVICTION_PERCENTAGE;

    protected String name;
    protected Object response;
    protected int completionId = MutableOperation.IGNORE_COMPLETION;

    protected transient boolean dontCreateCacheRecordStoreIfNotExist;
    protected transient EnterpriseSerializationService serializationService;
    protected transient EnterpriseCacheService cacheService;
    protected transient HiDensityCacheRecordStore cache;
    protected transient int partitionId;
    protected transient NativeOutOfMemoryError oome;

    protected AbstractHiDensityCacheOperation() {
    }

    protected AbstractHiDensityCacheOperation(String name) {
        this(name, MutableOperation.IGNORE_COMPLETION, false);
    }

    protected AbstractHiDensityCacheOperation(String name, boolean dontCreateCacheRecordStoreIfNotExist) {
        this(name, MutableOperation.IGNORE_COMPLETION, dontCreateCacheRecordStoreIfNotExist);
    }

    protected AbstractHiDensityCacheOperation(String name, int completionId) {
        this(name, completionId, false);
    }

    protected AbstractHiDensityCacheOperation(String name, int completionId,
                                              boolean dontCreateCacheRecordStoreIfNotExist) {
        this.name = name;
        this.completionId = completionId;
        this.dontCreateCacheRecordStoreIfNotExist = dontCreateCacheRecordStoreIfNotExist;
    }

    private void ensureInitialized() {
        if (cacheService == null) {
            cacheService = getService();
            serializationService = cacheService.getSerializationService();
        }
    }

    @Override
    public final void beforeRun() throws Exception {
        // No need to handle native memory OOME.
        // Native memory OOME is not possible because if there is not enough memory for reading operation data
        // into native memory, it is read into heap memory.
        // But if there is heap memory OOME,
        // there is no need to take an action since OOME handler will shutdown the node.

        ensureInitialized();

        partitionId = getPartitionId();
        try {
            if (dontCreateCacheRecordStoreIfNotExist) {
                cache = (HiDensityCacheRecordStore) cacheService.getRecordStore(name, partitionId);
            } else {
                cache = (HiDensityCacheRecordStore) cacheService.getOrCreateRecordStore(name, getPartitionId());
            }
        } catch (Throwable e) {
            dispose();
            throw ExceptionUtil.rethrow(e, Exception.class);
        }

        beforeRunInternal();
    }

    protected void beforeRunInternal() {

    }

    private int forceEvict() {
        return cacheService.forceEvict(name, getPartitionId());
    }

    private int forceEvictOnOthers() {
        return cacheService.forceEvictOnOthers(name, getPartitionId());
    }

    @Override
    public final void run() throws Exception {
        try {
            runInternal();
        } catch (NativeOutOfMemoryError e) {
            forceEvictAndRunInternal();
        }
    }

    private void forceEvictAndRunInternal() throws Exception {
        for (int i = 0; i < FORCED_EVICTION_RETRY_COUNT; i++) {
            try {
                forceEvict();
                runInternal();
                oome = null;
                break;
            } catch (NativeOutOfMemoryError e) {
                oome = e;
            }
        }

        if (oome != null) {
            for (int i = 0; i < FORCED_EVICTION_RETRY_COUNT; i++) {
                try {
                    forceEvictOnOthers();
                    runInternal();
                    oome = null;
                    break;
                } catch (NativeOutOfMemoryError e) {
                    oome = e;
                }
            }
        }

        if (oome != null) {
            dispose();
            throw oome;
        }
    }

    protected abstract void runInternal() throws Exception;

    protected final void dispose() {
        ensureInitialized();

        disposeDeferredBlocks();

        try {
            disposeInternal(serializationService);
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
            // TODO ignored error at the moment
            // a double free() error may be thrown if an operation fails
            // since internally key/value references are freed on oome
        }
    }

    private void disposeDeferredBlocks() {
        try {
            EnterpriseCacheService service = getService();
            HiDensityCacheRecordStore cache = (HiDensityCacheRecordStore) service.getRecordStore(name, getPartitionId());
            if (cache != null) {
                HiDensityRecordProcessor recordProcessor = cache.getRecordProcessor();
                recordProcessor.disposeDeferredBlocks();
            }
        } catch (Throwable e) {
            getLogger().warning("Error while freeing deferred memory blocks...", e);
        }
    }

    protected void disposeInternal(EnterpriseSerializationService serializationService) {

    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        disposeDeferredBlocks();
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public final Object getResponse() {
        return response;
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        dispose();
        super.onExecutionFailure(e);
    }

    @Override
    public void logError(Throwable e) {
        ILogger logger = getLogger();
        if (e instanceof NativeOutOfMemoryError) {
            Level level = this instanceof BackupOperation ? Level.FINEST : Level.WARNING;
            logger.log(level, "Cannot complete operation! -> " + e.getMessage());
        } else {
            super.logError(e);
        }
    }

    public int getCompletionId() {
        return completionId;
    }

    public void setCompletionId(int completionId) {
        this.completionId = completionId;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeInt(completionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        completionId = in.readInt();
    }

    @Override
    public String getServiceName() {
        return EnterpriseCacheService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    public static Data readHeapOperationData(ObjectDataInput in) throws IOException {
        return ((EnterpriseObjectDataInput) in).tryReadData(DataType.HEAP);
    }

    public static Data readNativeMemoryOperationData(ObjectDataInput in) throws IOException {
        return ((EnterpriseObjectDataInput) in).tryReadData(DataType.NATIVE);
    }

    public static Data readOperationData(ObjectDataInput in) throws IOException {
        return readNativeMemoryOperationData(in);
    }

}
