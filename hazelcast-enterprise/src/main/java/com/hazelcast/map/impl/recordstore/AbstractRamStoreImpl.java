package com.hazelcast.map.impl.recordstore;


import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;

public abstract class AbstractRamStoreImpl implements RamStore {

    protected final EnterpriseRecordStore recordStore;

    public AbstractRamStoreImpl(EnterpriseRecordStore recordStore) {
        this.recordStore = recordStore;
    }

    /**
     * Called while Hot Restart
     *
     * @param kh
     * @return key
     */
    public abstract Data createKey(KeyHandle kh);

    /**
     * Called while Hot Restart
     *
     * @param value can be null, this indicates created record will be a tombstone
     * @return a newly created record
     */
    public abstract Record createRecord(KeyHandle kh, Data value);

    @Override
    public final void accept(KeyHandle hrKey, byte[] valueBytes) {
        acceptInternal(hrKey, new HeapData(valueBytes), 0L);
    }

    private void acceptInternal(KeyHandle kh, Data value, long tombstoneSeq) {
        AbstractHotRestartStorageImpl<Record> storage = (AbstractHotRestartStorageImpl) recordStore.getStorage();
        Data key = createKey(kh);
        Record record = storage.getRecord(key);
        if (record == null) {
            record = createRecord(kh, value);
            storage.putTransient(key, record);
        } else {
            storage.updateTransient(key, record, value);
        }
        record.setTombstoneSequence(tombstoneSeq);
        recordStore.dispose();
    }

}
