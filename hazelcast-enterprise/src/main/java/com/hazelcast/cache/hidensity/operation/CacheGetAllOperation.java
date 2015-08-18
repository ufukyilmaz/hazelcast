package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ReadonlyOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Gets all keys from the cache.
 * <p>{@link com.hazelcast.cache.impl.operation.CacheGetAllOperationFactory} creates this operation.</p>
 * <p>Functionality: Filters out the partition keys and calls
 * {@link com.hazelcast.cache.impl.ICacheRecordStore#getAll(java.util.Set, javax.cache.expiry.ExpiryPolicy)}</p>
 */
public class CacheGetAllOperation
        extends PartitionWideCacheOperation
        implements ReadonlyOperation {

    private Set<Data> keys = new HashSet<Data>();
    private ExpiryPolicy expiryPolicy;

    public CacheGetAllOperation() {
    }

    public CacheGetAllOperation(String name, Set<Data> keys, ExpiryPolicy expiryPolicy) {
        super(name);
        this.keys = keys;
        this.expiryPolicy = expiryPolicy;
    }

    public void run() {
        CacheService service = getService();
        ICacheRecordStore cache = service.getOrCreateRecordStore(name, getPartitionId());

        int partitionId = getPartitionId();
        Set<Data> partitionKeySet = new HashSet<Data>();
        for (Data key : keys) {
            if (partitionId == getNodeEngine().getPartitionService().getPartitionId(key)) {
                partitionKeySet.add(key);
            }
        }
        response = cache.getAll(partitionKeySet, expiryPolicy);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        //TODO not used validate and remove !!
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                out.writeData(key);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        //TODO not used validate and remove !!
        super.readInternal(in);
        expiryPolicy = in.readObject();
        int size = in.readInt();
        if (size > -1) {
            for (int i = 0; i < size; i++) {
                Data key = in.readData();
                keys.add(key);
            }
        }
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.GET_ALL;
    }
}
