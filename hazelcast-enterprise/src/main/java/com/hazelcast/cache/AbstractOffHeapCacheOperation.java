package com.hazelcast.cache;

import com.hazelcast.cache.enterprise.EnterpriseCacheRecordStore;
import com.hazelcast.cache.enterprise.EnterpriseCacheService;
import com.hazelcast.cache.enterprise.impl.offheap.EnterpriseOffHeapCacheRecordStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
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
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.logging.Level;

/**
 * @author mdogan 05/02/14
 */
abstract class AbstractOffHeapCacheOperation
        extends AbstractOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    private static final int FORCED_EVICTION_RETRY_COUNT = 100 / EnterpriseCacheRecordStore.MIN_FORCED_EVICT_PERCENTAGE;

    String name;
    Data key;

    Object response;

    transient EnterpriseOffHeapCacheRecordStore cache;

    transient OffHeapOutOfMemoryError oome;

    protected AbstractOffHeapCacheOperation() {
    }

    protected AbstractOffHeapCacheOperation(String name, Data key) {
        this.name = name;
        this.key = key;
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
            if (this instanceof BackupAwareOffHeapCacheOperation) {
                cache = (EnterpriseOffHeapCacheRecordStore) service.getOrCreateCache(name, getPartitionId());
            } else {
                cache = (EnterpriseOffHeapCacheRecordStore) service.getCache(name, getPartitionId());
            }
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
        } catch (OffHeapOutOfMemoryError e) {
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
            } catch (OffHeapOutOfMemoryError e) {
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
            // TODO: ignored error at the moment
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
        if (e instanceof OffHeapOutOfMemoryError) {
            Level level = this instanceof BackupOperation ? Level.FINEST : Level.WARNING;
            logger.log(level, "Cannot complete operation! -> " + e.getMessage());
        } else {
            super.logError(e);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeData(key);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        key = readOffHeapData(in);
    }

    protected final Data readOffHeapData(ObjectDataInput in) throws IOException {
        try {
            return ((EnterpriseObjectDataInput) in).readData(DataType.OFFHEAP);
        } catch (OffHeapOutOfMemoryError e) {
            oome = e;
        }
        return null;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }
}
