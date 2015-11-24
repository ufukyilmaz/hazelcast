package com.hazelcast.map.impl.recordstore;


import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.Collection;

public abstract class AbstractRamStoreImpl implements RamStore {

    protected final EnterpriseRecordStore recordStore;

    public AbstractRamStoreImpl(EnterpriseRecordStore recordStore) {
        this.recordStore = recordStore;
    }

    /**
     * Called from a partition operation thread
     * @param keysToRelease
     */
    public abstract void releaseTombstonesInternal(Collection<TombstoneId> keysToRelease);

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
     * @param kh
     * @param value can be null, this indicates created record will be a tombstone
     * @return a newly created record
     */
    public abstract Record createRecord(KeyHandle kh, Data value);

    @Override
    public final void releaseTombstones(final Collection<TombstoneId> keysToRelease) {
        MapServiceContext mapServiceContext = recordStore.getMapContainer().getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();

        operationService.execute(new PartitionSpecificRunnable() {
            @Override
            public int getPartitionId() {
                return recordStore.getPartitionId();
            }

            @Override
            public void run() {
                releaseTombstonesInternal(keysToRelease);
            }
        });
    }

    @Override
    public final void accept(KeyHandle hrKey, byte[] valueBytes) {
        acceptInternal(hrKey, new HeapData(valueBytes), 0L);
    }

    @Override
    public final void acceptTombstone(KeyHandle hrKey, long seq) {
        acceptInternal(hrKey, null, seq);
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
