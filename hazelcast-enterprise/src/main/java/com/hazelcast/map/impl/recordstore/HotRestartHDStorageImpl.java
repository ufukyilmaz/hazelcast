package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordFactory;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.impl.KeyOffHeap;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Iterator;

/**
 * HotRestart storage implementation for maps configured with in-memory-format: {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 */
public class HotRestartHDStorageImpl extends HotRestartStorageImpl<HDRecord> implements ForcedEvictable<HDRecord> {

    private final Object mutex = new Object();

    public HotRestartHDStorageImpl(EnterpriseMapServiceContext mapServiceContext, RecordFactory recordFactory,
                                   InMemoryFormat inMemoryFormat, boolean fsync, long prefix) {
        super(mapServiceContext, recordFactory, inMemoryFormat, fsync, prefix);
    }

    @Override
    public HotRestartStore getHotRestartStore() {
        return mapServiceContext.getOffHeapHotRestartStoreForCurrentThread();
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat inMemoryFormat) {
        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        HiDensityRecordProcessor<HDRecord> recordProcessor = ((HDRecordFactory) recordFactory).getRecordProcessor();

        return new HDStorageImpl(recordProcessor, serializationService);
    }

    public void put(Data key, HDRecord record) {
        synchronized (mutex) {
            storage.put(key, record);
        }
        putToHotRestart(record);
    }

    @Override
    public void updateRecordValue(Data key, HDRecord record, Object val) {
        synchronized (mutex) {
            storage.updateRecordValue(key, record, val);
        }
        putToHotRestart(record);
    }

    @Override
    public void removeRecord(HDRecord record) {
        if (record == null) {
            return;
        }
        synchronized (mutex) {
            storage.removeRecord(record);
        }
        hotRestartStore.remove(createHotRestartKey(record), fsync);
    }

    @Override
    public void clear(boolean isDuringShutdown) {
        synchronized (mutex) {
            storage.clear(isDuringShutdown);
        }
        if (!isDuringShutdown) {
            hotRestartStore.clear(fsync, prefix);
        }
    }

    @Override
    public void destroy(boolean isDuringShutdown) {
        synchronized (mutex) {
            storage.destroy(isDuringShutdown);
        }
        if (!isDuringShutdown) {
            hotRestartStore.clear(fsync, prefix);
        }
    }

    @Override
    public HotRestartKey createHotRestartKey(HDRecord record) {
        NativeMemoryData key = (NativeMemoryData) record.getKey();
        return new KeyOffHeap(prefix, key.toByteArray(), key.address(), record.getSequence());
    }

    public long getNativeKeyAddress(Data key) {
        return getStorageImpl().getNativeKeyAddress(key);
    }

    public NativeMemoryData toNative(Data data) {
        return getStorageImpl().toNative(data);
    }

    public HDStorageImpl getStorageImpl() {
        return (HDStorageImpl) storage;
    }

    public Object getMutex() {
        return mutex;
    }

    @Override
    public Iterator<HDRecord> newForcedEvictionValuesIterator() {
        return ((ForcedEvictable<HDRecord>) storage).newForcedEvictionValuesIterator();
    }
}
