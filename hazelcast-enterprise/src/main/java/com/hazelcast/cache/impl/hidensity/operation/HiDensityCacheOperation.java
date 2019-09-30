package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.cache.impl.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.impl.operation.CacheOperation;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.internal.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.logging.Level;

import static com.hazelcast.cache.impl.AbstractCacheRecordStore.SOURCE_NOT_AVAILABLE;
import static java.lang.String.format;

abstract class HiDensityCacheOperation extends CacheOperation {

    static final int UNIT_PERCENTAGE = 100;
    static final int FORCED_EVICTION_RETRY_COUNT
            = UNIT_PERCENTAGE / HiDensityCacheRecordStore.DEFAULT_FORCED_EVICTION_PERCENTAGE;

    protected Object response;
    protected int completionId = MutableOperation.IGNORE_COMPLETION;

    protected transient boolean runCompleted;
    protected transient NativeOutOfMemoryError oome;
    protected transient EnterpriseSerializationService serializationService;

    protected HiDensityCacheOperation() {
    }

    protected HiDensityCacheOperation(String name) {
        this(name, MutableOperation.IGNORE_COMPLETION, false);
    }

    protected HiDensityCacheOperation(String name, boolean dontCreateCacheRecordStoreIfNotExist) {
        this(name, MutableOperation.IGNORE_COMPLETION, dontCreateCacheRecordStoreIfNotExist);
    }

    protected HiDensityCacheOperation(String name, int completionId) {
        this(name, completionId, false);
    }

    protected HiDensityCacheOperation(String name, int completionId, boolean dontCreateCacheRecordStoreIfNotExist) {
        super(name);
        this.completionId = completionId;
        this.dontCreateCacheRecordStoreIfNotExist = dontCreateCacheRecordStoreIfNotExist;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    public final Object getResponse() {
        return response;
    }

    protected void beforeRunInternal() {
        ensureInitialized();
    }

    private void ensureInitialized() {
        if (cacheService == null || serializationService == null) {
            cacheService = getService();
            serializationService = ((EnterpriseCacheService) cacheService).getSerializationService();
        }
    }

    @Override
    public final void run() throws Exception {
        try {
            runInternal();
        } catch (NativeOutOfMemoryError e) {
            forceEvictAndRunInternal();
        }
        runCompleted = true;
    }

    protected abstract void runInternal() throws Exception;

    @Override
    public void afterRun() throws Exception {
        try {
            super.afterRun();
        } finally {
            disposeDeferredBlocks();
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        dispose();
        super.onExecutionFailure(e);
    }

    @Override
    protected final void dispose() {
        ensureInitialized();

        disposeDeferredBlocks();

        try {
            disposeInternal(serializationService);
        } catch (Throwable e) {
            getLogger().warning("Error while disposing internal...", e);
            // TODO: ignored error at the moment
            // a double free() error may be thrown if an operation fails
            // since internally key/value references are freed on NOOME
        }
    }

    protected void disposeInternal(EnterpriseSerializationService serializationService) {
    }

    @Override
    public final void logError(Throwable e) {
        if (e instanceof NativeOutOfMemoryError) {
            Level level = this instanceof BackupOperation ? Level.FINEST : Level.WARNING;
            getLogger().log(level, "Cannot complete operation! -> " + e.getMessage());
            return;
        }

        super.logError(e);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(completionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        completionId = in.readInt();
    }

    public final int getCompletionId() {
        return completionId;
    }

    public final void setCompletionId(int completionId) {
        this.completionId = completionId;
    }

    private void forceEvictAndRunInternal() throws Exception {
        tryRunInternalByForceEviction();

        tryRunInternalByClearing();

        if (oome != null) {
            dispose();
            throw oome;
        }
    }

    private void tryRunInternalByForceEviction() throws Exception {
        ILogger logger = getLogger();

        for (int i = 0; i < FORCED_EVICTION_RETRY_COUNT; i++) {
            try {
                if (logger.isFineEnabled()) {
                    logger.fine(format("Applying forced eviction on current RecordStore (cache %s, partitionId: %d)!",
                            name, getPartitionId()));
                }
                // if there is NOOME, apply for eviction on current record store and try again
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
                    if (logger.isFineEnabled()) {
                        logger.fine(format("Applying forced eviction on other RecordStores owned by the same partition thread"
                                + " (cache %s, partitionId: %d", name, getPartitionId()));
                    }
                    // if still there is NOOME, apply for eviction on others and try again
                    forceEvictOnOthers();
                    runInternal();
                    oome = null;
                    break;
                } catch (NativeOutOfMemoryError e) {
                    oome = e;
                }
            }
        }
    }

    private void forceEvict() {
        ((EnterpriseCacheService) cacheService).forceEvict(name, getPartitionId());
    }

    private void forceEvictOnOthers() {
        ((EnterpriseCacheService) cacheService).forceEvictOnOthers(name, getPartitionId());
    }

    private void tryRunInternalByClearing() throws Exception {
        ILogger logger = getLogger();

        if (oome != null) {
            try {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Clearing current RecordStore because forced eviction was not enough!");
                }
                // if there is still NOOME, clear current record store and try again
                recordStore.clear();
                cacheService.sendInvalidationEvent(recordStore.getName(), null, SOURCE_NOT_AVAILABLE);
                runInternal();
                oome = null;
            } catch (NativeOutOfMemoryError e) {
                oome = e;
            }
        }

        if (oome != null) {
            try {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Clearing other RecordStores owned by the same partition thread"
                            + " because forced eviction was not enough!");
                }
                // if there still is NOOME, for the last chance, clear other record stores and try again
                ((EnterpriseCacheService) cacheService).clearAll(getPartitionId());
                runInternal();
                oome = null;
            } catch (NativeOutOfMemoryError e) {
                oome = e;
            }
        }
    }

    private void disposeDeferredBlocks() {
        try {
            EnterpriseCacheService service = getService();
            HiDensityCacheRecordStore recordStore = (HiDensityCacheRecordStore) service.getRecordStore(name, getPartitionId());
            if (recordStore != null) {
                recordStore.disposeDeferredBlocks();
            }
        } catch (Throwable e) {
            getLogger().warning("Error while freeing deferred memory blocks...", e);
        }
    }

    public static Data readHeapOperationData(ObjectDataInput in) throws IOException {
        return ((EnterpriseObjectDataInput) in).tryReadData(DataType.HEAP);
    }

    public static Data readNativeMemoryOperationData(ObjectDataInput in) throws IOException {
        return ((EnterpriseObjectDataInput) in).tryReadData(DataType.NATIVE);
    }
}
