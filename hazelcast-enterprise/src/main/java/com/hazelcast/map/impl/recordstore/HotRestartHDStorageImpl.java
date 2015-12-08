package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordFactory;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.impl.KeyOffHeap;

import static com.hazelcast.map.impl.recordstore.HDStorageImpl.DEFAULT_CAPACITY;
import static com.hazelcast.map.impl.recordstore.HDStorageImpl.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * HotRestart storage implementation for maps configured with in-memory-format: {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 */
public class HotRestartHDStorageImpl extends AbstractHotRestartStorageImpl<HDRecord> {

    private final Object mutex = new Object();

    public HotRestartHDStorageImpl(EnterpriseMapServiceContext mapServiceContext, RecordFactory recordFactory,
                                   InMemoryFormat inMemoryFormat, long prefix) {
        super(mapServiceContext, recordFactory, inMemoryFormat, prefix);
    }

    @Override
    public HotRestartStore getHotRestartStore() {
        return mapServiceContext.getOffHeapHotRestartStoreForCurrentThread();
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat inMemoryFormat) {
        HiDensityRecordProcessor<HDRecord> recordProcessor = ((HDRecordFactory) recordFactory).getRecordProcessor();
        HotRestartSampleableHDRecordMap sampleableHDRecordMap = new HotRestartSampleableHDRecordMap(DEFAULT_CAPACITY,
                DEFAULT_LOAD_FACTOR, recordProcessor);
        return new HDStorageImpl(sampleableHDRecordMap, recordProcessor);
    }

    @Override
    public void removeTransient(HDRecord record) {
        synchronized (mutex) {
            super.removeTransient(record);
        }
    }

    @Override
    public void clear() {
        synchronized (mutex) {
            super.clear();
        }
    }

    @Override
    public void putInternal(Data key, HDRecord record) {
        synchronized (mutex) {
            storage.put(key, record);
        }
    }

    @Override
    public void updateInternal(Data key, HDRecord record, Object val) {
        synchronized (mutex) {
            storage.updateRecordValue(key, record, val);
        }
    }

    @Override
    public void removeInternal(HDRecord record) {
        getStorageImpl().addDeferredDispose(record.getValue());
        synchronized (mutex) {
            record.setValueAddress(NULL_ADDRESS);
        }
    }

    public Object getMutex() {
        return mutex;
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
}
