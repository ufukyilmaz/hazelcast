package com.hazelcast.map.impl.recordstore.expiry;

import com.hazelcast.internal.elastic.CapacityUtil;
import com.hazelcast.internal.elastic.map.BinaryElasticHashMap;
import com.hazelcast.internal.elastic.map.NativeBehmSlotAccessorFactory;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Specialized implementation of {@link
 * ExpirySystem} to be used with {@link
 * com.hazelcast.config.InMemoryFormat#NATIVE} memory backed maps.
 *
 * @see ExpirySystem
 */
public class HDExpirySystem extends ExpirySystem {
    private EnterpriseSerializationService ess;

    private final HiDensityStorageInfo hdStorageInfo;
    // Used to prevent temporary object creation while
    // checking expiry This can be millions of object in some
    // cases like full scan of entry-set at query executions
    private final HDExpiryMetadata hdExpiryMetadata = new HDExpiryMetadata();

    private DefaultHiDensityRecordProcessor memoryBlockProcessor;

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    public HDExpirySystem(RecordStore recordStore, MapContainer mapContainer,
                          MapServiceContext mapServiceContext) {
        super(recordStore, mapContainer, mapServiceContext);
        SerializationService ss = mapServiceContext.getNodeEngine().getSerializationService();
        this.ess = (EnterpriseSerializationService) ss;
        this.hdStorageInfo = ((EnterpriseMapContainer) mapContainer).getHDStorageInfo();
    }

    @Override
    protected Map<Data, ExpiryMetadata> createExpiryTimeByKeyMap() {
        HDExpiryMetadataAccessor accessor = new HDExpiryMetadataAccessor(ess);
        NativeBehmSlotAccessorFactory behmSlotAccessorFactory = new NativeBehmSlotAccessorFactory();
        memoryBlockProcessor = new DefaultHiDensityRecordProcessor(ess,
                accessor, ess.getMemoryManager(), hdStorageInfo);
        return new BinaryElasticHashMap(CapacityUtil.DEFAULT_CAPACITY,
                behmSlotAccessorFactory,
                memoryBlockProcessor);
    }

    @Override
    protected ExpiryMetadata createExpiryMetadata(long ttlMillis, long maxIdleMillis, long expirationTime) {
        assert memoryBlockProcessor != null;
        long address = memoryBlockProcessor.allocate(HDExpiryMetadata.SIZE);
        return new HDExpiryMetadata(address)
                .setTtl(ttlMillis)
                .setMaxIdle(maxIdleMillis)
                .setExpirationTime(expirationTime);
    }

    @Override
    protected ExpiryMetadata getExpiryMetadataForExpiryCheck(Data key,
                                                             Map<Data, ExpiryMetadata> expireTimeByKey) {
        long nativeValueAddress = ((BinaryElasticHashMap) expireTimeByKey).getNativeValueAddress(key);
        if (nativeValueAddress == NULL_ADDRESS) {
            return ExpiryMetadata.NULL;
        }
        return hdExpiryMetadata.reset(nativeValueAddress);
    }

    @Override
    protected Iterator<Map.Entry<Data, ExpiryMetadata>> initIteratorOf(Map<Data, ExpiryMetadata> expireTimeByKey) {
        BinaryElasticHashMap map = (BinaryElasticHashMap) expireTimeByKey;
        return map.entryIter(false);
    }

    @Override
    protected void callIterRemove(Iterator<Map.Entry<Data, ExpiryMetadata>> expirationIterator) {
        ((BinaryElasticHashMap.SlotIter) expirationIterator).removeInternal(false);
    }

    @Override
    protected void callRemove(Data key, Map<Data, ExpiryMetadata> expireTimeByKey) {
        assert key instanceof NativeMemoryData;

        BinaryElasticHashMap map = (BinaryElasticHashMap) expireTimeByKey;
        map.delete(key);
    }

    @Override
    public void clear() {
        Map<Data, ExpiryMetadata> map = getOrCreateExpireTimeByKeyMap(false);
        if (map instanceof BinaryElasticHashMap) {
            //We create HD keys and records in `storage.put`.
            //Expiry system uses these HD keys not to create
            //new ones redundantly. During `clear`, these keys
            //should not be disposed by expirySystem because
            //this is store.clear method's responsibility. This
            //is why we have clearWithoutKeyDisposal here.
            ((BinaryElasticHashMap) map).clearWithoutKeyDisposal();
        }
    }

    @Override
    public void destroy() {
        Map<Data, ExpiryMetadata> map = getOrCreateExpireTimeByKeyMap(false);
        if (map instanceof BinaryElasticHashMap) {
            ((BinaryElasticHashMap) map).dispose();
        }
    }
}
