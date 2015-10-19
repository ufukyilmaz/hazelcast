package com.hazelcast.cache.hidensity.nearcache;

import com.hazelcast.cache.hidensity.nearcache.impl.nativememory.HiDensitySegmentedNativeMemoryNearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.impl.DefaultNearCache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.util.EmptyStatement;

import java.util.Collection;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@link com.hazelcast.cache.impl.nearcache.NearCache} implementation for Hi-Density cache.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 * @author sozal 26/10/14
 */
public class HiDensityNearCache<K, V> extends DefaultNearCache<K, V> {

    private final NearCacheManager nearCacheManager;
    private MemoryManager memoryManager;

    public HiDensityNearCache(String s, NearCacheConfig nearCacheConfig,
                              NearCacheContext nearCacheContext) {
        super(s, nearCacheConfig, nearCacheContext);
        this.nearCacheManager = nearCacheContext.getNearCacheManager();
    }

    public HiDensityNearCache(String name, NearCacheConfig nearCacheConfig,
                              NearCacheContext nearCacheContext,
                              NearCacheRecordStore<K, V> nearCacheRecordStore) {
        super(name, nearCacheConfig, nearCacheContext, nearCacheRecordStore);
        this.nearCacheManager = nearCacheContext.getNearCacheManager();
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
    protected void init() {
        super.init();
        if (nearCacheRecordStore instanceof HiDensityNearCacheRecordStore) {
            memoryManager = ((HiDensityNearCacheRecordStore) nearCacheRecordStore).getMemoryManager();
        }
    }

    @Override
    public void put(K key, V value) {
        checkNotNull(key, "key cannot be null on put!");

        NativeOutOfMemoryError oomeError;
        boolean anyAvailableNearCacheToEvict;

        do {
            anyAvailableNearCacheToEvict = false;

            try {
                // Try to put new record to near-cache
                super.put(key, value);
                oomeError = null;
                break;
            } catch (NativeOutOfMemoryError oome) {
                oomeError = oome;
            }

            // If there is any record, this means that this near-cache si a candidate for eviction
            if (nearCacheRecordStore.size() > 0) {
                try {
                    anyAvailableNearCacheToEvict = true;
                    // Evict a record from this near-cache regardless from eviction max-size policy
                    nearCacheRecordStore.doEviction();
                    // Try to put new record to this near-cache after eviction
                    super.put(key, value);
                    oomeError = null;
                    break;
                } catch (NativeOutOfMemoryError oome) {
                    oomeError = oome;
                }
            }

            try {
                if (tryToPutByEvictingOnOtherNearCaches(key, value)) {
                    // There is no OOME and eviction is done.
                    // This means that record successfully put to near-cache
                    oomeError = null;
                    anyAvailableNearCacheToEvict = true;
                    break;
                }
            } catch (NativeOutOfMemoryError oome) {
                anyAvailableNearCacheToEvict = true;
                oomeError = oome;
            }

            // If still put cannot be done and there are evictable near-caches, keep on trying.
        } while (anyAvailableNearCacheToEvict);

        checkAndHandleOOME(key, value, oomeError);
    }


    private boolean tryToPutByEvictingOnOtherNearCaches(K key, V value) {
        if (nearCacheManager == null) {
            return false;
        }
        NativeOutOfMemoryError oomeError = null;
        boolean anyOtherAvailableNearCacheToEvict = false;
        Collection<NearCache> nearCacheList = nearCacheManager.listAllNearCaches();
        for (NearCache nearCache : nearCacheList) {
            if (nearCache != this && nearCache instanceof HiDensityNearCache && nearCache.size() > 0) {
                HiDensityNearCache hiDensityNearCache = (HiDensityNearCache) nearCache;
                try {
                    // Evict a record regardless from eviction max-size policy
                    hiDensityNearCache.nearCacheRecordStore.doEviction();
                    anyOtherAvailableNearCacheToEvict = true;
                    // Try to put new record to near-cache after eviction
                    super.put(key, value);
                    oomeError = null;
                    break;
                } catch (NativeOutOfMemoryError oome) {
                    oomeError = oome;
                } catch (IllegalStateException e) {
                    // Near-cache may be destroyed at this time, so just ignore exception
                    EmptyStatement.ignore(e);
                }
            }
        }
        if (oomeError != null) {
            throw oomeError;
        }
        return anyOtherAvailableNearCacheToEvict;
    }

    private void checkAndHandleOOME(K key, V value, NativeOutOfMemoryError oomeError) {
        if (oomeError != null) {
            if (memoryManager != null) {
                memoryManager.compact();
                // Try for last time after compaction
                super.put(key, value);
            } else {
                throw oomeError;
            }
        }
    }

}
