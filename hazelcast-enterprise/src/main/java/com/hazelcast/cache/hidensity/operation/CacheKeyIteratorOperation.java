package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.hidensity.HiDensityCacheRecord;
import com.hazelcast.cache.impl.CacheKeyIterationResult;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @deprecated OSS version ({@link com.hazelcast.cache.impl.operation.CacheKeyIteratorOperation}) is enough,
 * no need for this EE version
 */
@Deprecated
public class CacheKeyIteratorOperation extends AbstractHiDensityCacheOperation implements ReadonlyOperation {

    private int slot;
    private int batch;

    public CacheKeyIteratorOperation() {
    }

    public CacheKeyIteratorOperation(String name, int slot, int batch) {
        super(name, true);
        this.slot = slot;
        this.batch = batch;
    }

    @Override
    protected void runInternal() throws Exception {
        if (cache != null) {
            long now = Clock.currentTimeMillis();
            SlottableIterator<Map.Entry<Data, HiDensityCacheRecord>> iter = cache.iterator(slot);
            List<Data> keys = new ArrayList<Data>();
            int count = 0;
            while (iter.hasNext()) {
                Map.Entry<Data, HiDensityCacheRecord> entry = iter.next();
                Data key = entry.getKey();
                HiDensityCacheRecord record = entry.getValue();
                if (record.isExpiredAt(now)) {
                    continue;
                }

                keys.add(serializationService.convertData(key, DataType.HEAP));
                if (++count == batch) {
                    break;
                }
            }
            int newSlot = iter.getNextSlot();
            response = new CacheKeyIterationResult(keys, newSlot);
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
