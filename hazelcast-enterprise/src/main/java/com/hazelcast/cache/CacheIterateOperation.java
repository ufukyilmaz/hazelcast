package com.hazelcast.cache;

import com.hazelcast.cache.enterprise.impl.offheap.EnterpriseOffHeapCacheRecord;
import com.hazelcast.cache.enterprise.impl.offheap.EnterpriseOffHeapCacheRecordStore;
import com.hazelcast.cache.enterprise.EnterpriseCacheService;
import com.hazelcast.elasticcollections.map.BinaryOffHeapHashMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.OffHeapData;

import java.io.IOException;
import java.util.Map;

/**
 * @author mdogan 15/05/14
 */
public class CacheIterateOperation extends PartitionWideCacheOperation {

    private int slot;
    private int batch;

    public CacheIterateOperation() {
    }

    public CacheIterateOperation(String name, int slot, int batch) {
        super(name);
        this.slot = slot;
        this.batch = batch;
    }

    @Override
    public void run() throws Exception {
        EnterpriseCacheService service = getService();
        EnterpriseOffHeapCacheRecordStore cache =
                (EnterpriseOffHeapCacheRecordStore) service.getCache(name, getPartitionId());
        if (cache != null) {
            EnterpriseSerializationService ss = service.getSerializationService();
            BinaryOffHeapHashMap<EnterpriseOffHeapCacheRecord>.EntryIter iter = cache.iterator(slot);
            Data[] keys = new Data[batch];
            Data[] values = new Data[batch];
            int count = 0;
            while (iter.hasNext()) {
                Map.Entry<Data, EnterpriseOffHeapCacheRecord> entry = iter.next();
                Data key = entry.getKey();
                keys[count] = ss.convertData(key, DataType.HEAP);
                EnterpriseOffHeapCacheRecord record = entry.getValue();
                OffHeapData value = cache.getCacheRecordService().readData(record.getValueAddress());
                values[count] = ss.convertData(value, DataType.HEAP);
                if (++count == batch) {
                    break;
                }
            }
            int newSlot = iter.getNextSlot();
            response = new CacheIterationResult(keys, values, getPartitionId(), newSlot, count);
        }
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.ITERATE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(slot);
        out.writeInt(batch);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        slot = in.readInt();
        batch = in.readInt();
    }
}
