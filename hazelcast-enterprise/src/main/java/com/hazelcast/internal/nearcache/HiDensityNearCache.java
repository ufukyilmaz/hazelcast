package com.hazelcast.internal.nearcache;

import com.hazelcast.internal.nearcache.impl.nativememory.HiDensitySegmentedNativeMemoryNearCacheRecordStore;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.EmptyStatement;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@link com.hazelcast.internal.nearcache.NearCache} implementation for Hi-Density cache.
 *
 * @param <K> the type of the key.
 * @param <V> the type of the value.
 */
public class HiDensityNearCache<K, V> extends DefaultNearCache<K, V> {

    private final ILogger logger = Logger.getLogger(getClass());
    private final NearCacheManager nearCacheManager;
    private HazelcastMemoryManager memoryManager;

    public HiDensityNearCache(String name, NearCacheConfig nearCacheConfig, NearCacheManager nearCacheManager,
                              EnterpriseSerializationService serializationService, ExecutionService executionService,
                              ClassLoader classLoader) {
        this(name, nearCacheConfig, nearCacheManager, null, serializationService, executionService, classLoader);
    }

    public HiDensityNearCache(String name, NearCacheConfig nearCacheConfig, NearCacheManager nearCacheManager,
                              NearCacheRecordStore<K, V> nearCacheRecordStore, SerializationService serializationService,
                              ExecutionService executionService, ClassLoader classLoader) {
        super(name, nearCacheConfig, nearCacheRecordStore, serializationService, executionService, classLoader);
        this.nearCacheManager = nearCacheManager;
    }

    @Override
    public void initialize() {
        super.initialize();

        memoryManager = createMemoryManager();
        nearCacheRecordStore.initialize();
    }

    private HazelcastMemoryManager createMemoryManager() {
        if (nearCacheRecordStore instanceof HiDensityNearCacheRecordStore) {
            return ((HiDensityNearCacheRecordStore) nearCacheRecordStore).getMemoryManager();
        }
        return null;
    }

    @Override
    protected NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig) {
        if (NATIVE == nearCacheConfig.getInMemoryFormat()) {
            EnterpriseSerializationService ss = (EnterpriseSerializationService) serializationService;
            return new HiDensitySegmentedNativeMemoryNearCacheRecordStore<K, V>(nearCacheConfig, ss, classLoader);
        }

        return super.createNearCacheRecordStore(nearCacheConfig);
    }

    @Override
    public void put(K key, V value) {
        checkNotNull(key, "key cannot be null on put!");

        NativeOutOfMemoryError oomeError;
        boolean anyAvailableNearCacheToEvict;

        do {
            anyAvailableNearCacheToEvict = false;

            try {
                // try to put new record to Near Cache
                super.put(key, value);
                oomeError = null;
                break;
            } catch (NativeOutOfMemoryError oome) {
                oomeError = oome;
            }

            // if there is any record, this means that this Near Cache is a candidate for eviction
            if (nearCacheRecordStore.size() > 0) {
                try {
                    anyAvailableNearCacheToEvict = true;
                    // evict a record from this Near Cache regardless from eviction max-size policy
                    nearCacheRecordStore.doEviction();
                    // try to put new record to this Near Cache after eviction
                    super.put(key, value);
                    oomeError = null;
                    break;
                } catch (NativeOutOfMemoryError oome) {
                    oomeError = oome;
                }
            }

            try {
                if (tryToPutByEvictingOnOtherNearCaches(key, value)) {
                    // there is no OOME and eviction is done, this means that record successfully put to Near Cache
                    oomeError = null;
                    break;
                }
            } catch (NativeOutOfMemoryError oome) {
                anyAvailableNearCacheToEvict = true;
                oomeError = oome;
            }

            // if put still cannot be done and there are evictable Near Caches, keep on trying
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
                    // evict a record regardless from eviction max-size policy
                    hiDensityNearCache.nearCacheRecordStore.doEviction();
                    anyOtherAvailableNearCacheToEvict = true;
                    // try to put new record to Near Cache after eviction
                    super.put(key, value);
                    oomeError = null;
                    break;
                } catch (NativeOutOfMemoryError oome) {
                    oomeError = oome;
                } catch (IllegalStateException e) {
                    // Near Cache may be destroyed at this time, so just ignore exception
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
        if (oomeError == null) {
            return;
        }

        assert memoryManager != null : "memoryManager cannot be null";

        memoryManager.compact();

        // try for last time after compaction
        try {
            super.put(key, value);
        } catch (NativeOutOfMemoryError e) {
            // there may be an existing entry in Near Cache for the specified `key`, to be in safe side, remove that entry,
            // otherwise stale value for that `key` may be seen indefinitely. This removal will make subsequent gets to fetch
            // the value from underlying IMap/cache
            super.remove(key);
            // due to the ongoing compaction, one user thread may not see sufficient space to put entry into Near Cache;
            // in that case, skipping NativeOutOfMemoryError instead of throwing it to user (even eviction is configured)
            // this is because Near Cache feature is an optimization and it is okay not to put some entries;
            // we are expecting to put next entries into Near Cache after compaction or after Near Cache invalidation
            logger.warning("Entry can not be put into Near Cache for this time");
        }
    }
}
