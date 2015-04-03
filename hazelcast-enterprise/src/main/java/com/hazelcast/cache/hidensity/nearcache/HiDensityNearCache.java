package com.hazelcast.cache.hidensity.nearcache;

import com.hazelcast.cache.hidensity.nearcache.impl.nativememory.HiDensitySegmentedNativeMemoryNearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.impl.DefaultNearCache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.hidensity.HiDensityRecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;

/**
 * {@link com.hazelcast.cache.impl.nearcache.NearCache} implementation for Hi-Density cache.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 *
 * @author sozal 26/10/14
 */
public class HiDensityNearCache<K, V> extends DefaultNearCache<K, V> {

    public HiDensityNearCache(String s, NearCacheConfig nearCacheConfig,
                              NearCacheContext nearCacheContext) {
        super(s, nearCacheConfig, nearCacheContext);
    }

    public HiDensityNearCache(String name, NearCacheConfig nearCacheConfig,
                              NearCacheContext nearCacheContext,
                              NearCacheRecordStore<K, V> nearCacheRecordStore) {
        super(name, nearCacheConfig, nearCacheContext, nearCacheRecordStore);
    }
    @Override
    protected NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                                    NearCacheContext nearCacheContext) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat == InMemoryFormat.NATIVE) {
            return new HiDensitySegmentedNativeMemoryNearCacheRecordStore<K, V>(nearCacheConfig, nearCacheContext);
        }
        return super.createNearCacheRecordStore(nearCacheConfig, nearCacheContext);
    }

    @Override
    public void put(K key, V value) {
        putInternal(key, value);
    }

    private void putInternal(K key, V value) {
        NativeOutOfMemoryError oomeError = null;
        for (int i = 0; i < HiDensityRecordStore.FORCE_EVICTION_TRY_COUNT; i++) {
            try {
                super.put(key, value);
                oomeError = null;
                break;
            } catch (NativeOutOfMemoryError oome) {
                oomeError = oome;
                // If there is still OOME and it is possible to clear some records, do force eviction
                if (nearCacheRecordStore instanceof HiDensityNearCacheRecordStore && nearCacheRecordStore.size() > 0) {
                    HiDensityNearCacheRecordStore hiDensityNearCacheRecordStore =
                            (HiDensityNearCacheRecordStore) nearCacheRecordStore;
                    hiDensityNearCacheRecordStore.forceEvict();
                } else {
                    break;
                }
            }
        }
        if (oomeError != null) {
            throw oomeError;
        }
    }

}
