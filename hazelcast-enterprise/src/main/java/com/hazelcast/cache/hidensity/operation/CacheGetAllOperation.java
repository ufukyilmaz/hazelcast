package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.partition.InternalPartitionService;
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
        extends AbstractHiDensityCacheOperation
        implements ReadonlyOperation {

    private static final MapEntrySet EMPTY_ENTRY_SET = new MapEntrySet();

    private Set<Data> keys = new HashSet<Data>();
    private ExpiryPolicy expiryPolicy;

    public CacheGetAllOperation() {
    }

    public CacheGetAllOperation(String name, Set<Data> keys, ExpiryPolicy expiryPolicy) {
        super(name, true);
        this.keys = keys;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void runInternal() throws Exception {
        if (cache != null) {
            Set<Data> partitionKeySet = new HashSet<Data>();
            InternalPartitionService partitionService = getNodeEngine().getPartitionService();
            for (Data key : keys) {
                if (partitionId == partitionService.getPartitionId(key)) {
                    partitionKeySet.add(key);
                }
            }
            response = cache.getAll(partitionKeySet, expiryPolicy);
        } else {
            response = EMPTY_ENTRY_SET;
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        dispose();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        for (Data key : keys) {
            serializationService.disposeData(key);
        }
    }

    // Currently, since `CacheGetAllOperation` is created by `CacheGetAllOperationFactory`,
    // it is not sent over network and not serialized.
    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        // TODO not used validate and remove
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

    // Currently, since `CacheGetAllOperation` is created by `CacheGetAllOperationFactory`,
    // it is not received over network and not deserialized.
    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        // TODO not used validate and remove
        super.readInternal(in);
        expiryPolicy = in.readObject();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Data key = AbstractHiDensityCacheOperation.readNativeMemoryOperationData(in);
            keys.add(key);
        }
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.GET_ALL;
    }

}
