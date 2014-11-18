package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.logging.Level;

/**
 * @author mdogan 05/02/14
 */
abstract class AbstractHiDensityCacheOperation
        extends AbstractOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable, MutableOperation {

    protected static final int FORCED_EVICTION_RETRY_COUNT =
            ICacheRecordStore.ONE_HUNDRED_PERCENT / ICacheRecordStore.MIN_FORCED_EVICT_PERCENTAGE;

    protected String name;
    protected Data key;
    protected Object response;
    protected int completionId = MutableOperation.IGNORE_COMPLETION;

    protected transient HiDensityCacheRecordStore cache;
    protected transient NativeOutOfMemoryError oome;

    protected AbstractHiDensityCacheOperation() {
    }

    protected AbstractHiDensityCacheOperation(String name) {
        this.name = name;
    }

    protected AbstractHiDensityCacheOperation(String name, Data key) {
        this.name = name;
        this.key = key;
    }

    protected AbstractHiDensityCacheOperation(String name, int completionId) {
        this.name = name;
        this.completionId = completionId;
    }

    protected AbstractHiDensityCacheOperation(String name, Data key, int completionId) {
        this.name = name;
        this.key = key;
        this.completionId = completionId;
    }

    @Override
    public final void beforeRun() throws Exception {
        if (oome != null) {
            dispose();
            forceEvict();
            throw oome;
        }

        try {
            EnterpriseCacheService service = getService();
            cache = (HiDensityCacheRecordStore) service.getOrCreateCache(name, getPartitionId());
            // This is commented-out since some TCK tests requires created cache
            // if there is no cache with specified partition id (or key) for cache miss statistics
            /*
            if (this instanceof BackupAwareOffHeapCacheOperation) {
                cache = (HiDensityNativeMemoryCacheRecordStore) service.getOrCreateCache(name, getPartitionId());
            } else {
                cache = (HiDensityNativeMemoryCacheRecordStore) service.getCacheRecordStore(name, getPartitionId());
            }
            */
        } catch (Throwable e) {
            dispose();
            throw ExceptionUtil.rethrow(e, Exception.class);
        }
    }

    private int forceEvict() {
        EnterpriseCacheService service = getService();
        return service.forceEvict(name, getPartitionId());
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
            dispose();
            throw oome;
        }
    }

    public abstract void runInternal() throws Exception;

    protected final void dispose() {
        try {
            SerializationService ss = getNodeEngine().getSerializationService();
            if (key != null) {
                ss.disposeData(key);
            }
            disposeInternal(ss);
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
            // TODO ignored error at the moment
            // a double free() error may be thrown if an operation fails
            // since internally key/value references are freed on oome
        }
    }

    protected abstract void disposeInternal(SerializationService binaryService);

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public final Object getResponse() {
        return response;
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
    public int getCompletionId() {
        return completionId;
    }

    @Override
    public void setCompletionId(int completionId) {
        this.completionId = completionId;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeData(key);
        out.writeInt(completionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        key = readNativeData(in);
        completionId = in.readInt();
    }

    protected final Data readNativeData(ObjectDataInput in) throws IOException {
        try {
            return ((EnterpriseObjectDataInput) in).readData(DataType.NATIVE);
        } catch (NativeOutOfMemoryError e) {
            oome = e;
        }
        return null;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }
}
