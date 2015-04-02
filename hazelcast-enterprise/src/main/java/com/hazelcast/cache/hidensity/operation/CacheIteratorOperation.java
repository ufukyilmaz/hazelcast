package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.hidensity.HiDensityCacheRecord;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.io.IOException;
import java.util.Map;

/**
 * @author mdogan 15/05/14
 */
public class CacheIteratorOperation extends PartitionWideCacheOperation {

    private int slot;
    private int batch;

    public CacheIteratorOperation() {
    }

    public CacheIteratorOperation(String name, int slot, int batch) {
        super(name);
        this.slot = slot;
        this.batch = batch;
    }

    @Override
    public void run() throws Exception {
        EnterpriseCacheService service = getService();
        HiDensityCacheRecordStore cache =
                (HiDensityCacheRecordStore) service.getCacheRecordStore(name, getPartitionId());
        if (cache != null) {
            EnterpriseSerializationService ss = service.getSerializationService();
            SlottableIterator<Map.Entry<Data, HiDensityCacheRecord>> iter = cache.iterator(slot);
            Data[] keys = new Data[batch];
            Data[] values = new Data[batch];
            int count = 0;
            while (iter.hasNext()) {
                Map.Entry<Data, HiDensityCacheRecord> entry = iter.next();
                Data key = entry.getKey();
                keys[count] = ss.convertData(key, DataType.HEAP);
                HiDensityCacheRecord record = entry.getValue();
                Data value = cache.getRecordProcessor().readData(record.getValueAddress());
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

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.ITERATE;
    }
}
