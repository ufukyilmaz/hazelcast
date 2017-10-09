package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.AbstractCacheRecordStore;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.logging.Level;

import static java.lang.String.format;

abstract class AbstractHiDensityCacheOperation
        extends AbstractNamedOperation
        implements PartitionAwareOperation, ServiceNamespaceAware, IdentifiedDataSerializable {

    static final int FORCED_EVICTION_RETRY_COUNT
            = ICacheRecordStore.UNIT_PERCENTAGE / HiDensityCacheRecordStore.DEFAULT_FORCED_EVICTION_PERCENTAGE;

    protected Object response;
    protected int completionId = MutableOperation.IGNORE_COMPLETION;

    protected transient boolean dontCreateCacheRecordStoreIfNotExist;
    protected transient EnterpriseSerializationService serializationService;
    protected transient EnterpriseCacheService cacheService;
    protected transient HiDensityCacheRecordStore cache;
    protected transient int partitionId;
    protected transient NativeOutOfMemoryError oome;
    protected transient boolean runCompleted;

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

    protected AbstractHiDensityCacheOperation(String name, int completionId, boolean dontCreateCacheRecordStoreIfNotExist) {
        super(name);
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
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    public String getServiceName() {
        return EnterpriseCacheService.SERVICE_NAME;
    }

    @Override
    public ServiceNamespace getServiceNamespace() {
        ICacheRecordStore recordStore = cache;
        if (recordStore == null) {
            EnterpriseCacheService service = getService();
            recordStore = service.getOrCreateRecordStore(name, getPartitionId());
        }
        return recordStore.getObjectNamespace();
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
    public final void beforeRun() throws Exception {
        // no need to handle NativeOutOfMemoryError, since NOOME is not possible here
        // (if there is not enough memory for reading operation data into native memory, it is read into heap memory,
        // but if there is a heap OOME, there is no need to take an action since OOME handler will shutdown the node)

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
        super.afterRun();

        disposeDeferredBlocks();
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        dispose();
        super.onExecutionFailure(e);
    }

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
    public void logError(Throwable e) {
        ILogger logger = getLogger();
        if (e instanceof NativeOutOfMemoryError) {
            Level level = this instanceof BackupOperation ? Level.FINEST : Level.WARNING;
            logger.log(level, "Cannot complete operation! -> " + e.getMessage());
        } else {
            super.logError(e);
        }
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

    public int getCompletionId() {
        return completionId;
    }

    public void setCompletionId(int completionId) {
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
        cacheService.forceEvict(name, getPartitionId());
    }

    private void forceEvictOnOthers() {
        cacheService.forceEvictOnOthers(name, getPartitionId());
    }

    private void tryRunInternalByClearing() throws Exception {
        ILogger logger = getLogger();

        if (oome != null) {
            try {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Clearing current RecordStore because forced eviction was not enough!");
                }
                // if there is still NOOME, clear current record store and try again
                cache.clear();
                cacheService.sendInvalidationEvent(cache.getName(), null, AbstractCacheRecordStore.SOURCE_NOT_AVAILABLE);
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
                cacheService.clearAll(getPartitionId());
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
            HiDensityCacheRecordStore cache = (HiDensityCacheRecordStore) service.getRecordStore(name, getPartitionId());
            if (cache != null) {
                HiDensityRecordProcessor recordProcessor = cache.getRecordProcessor();
                recordProcessor.disposeDeferredBlocks();
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
