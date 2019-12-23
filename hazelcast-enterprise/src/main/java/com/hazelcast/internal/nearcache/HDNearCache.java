package com.hazelcast.internal.nearcache;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.internal.nearcache.impl.nativememory.SegmentedHDNearCacheRecordStore;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.logging.Level.WARNING;

/**
 * {@link com.hazelcast.internal.nearcache.NearCache} implementation for Hi-Density cache.
 *
 * @param <K> the type of the key.
 * @param <V> the type of the value.
 */
public class HDNearCache<K, V> extends DefaultNearCache<K, V> {

    private final ILogger logger = Logger.getLogger(getClass());
    private final NearCacheManager nearCacheManager;
    private HazelcastMemoryManager memoryManager;

    public HDNearCache(String name, NearCacheConfig nearCacheConfig, NearCacheManager nearCacheManager,
                       EnterpriseSerializationService serializationService, TaskScheduler scheduler,
                       ClassLoader classLoader, HazelcastProperties properties) {
        this(name, nearCacheConfig, nearCacheManager, null,
                serializationService, scheduler, classLoader, properties);
    }

    public HDNearCache(String name, NearCacheConfig nearCacheConfig, NearCacheManager nearCacheManager,
                       NearCacheRecordStore<K, V> nearCacheRecordStore, SerializationService serializationService,
                       TaskScheduler scheduler, ClassLoader classLoader, HazelcastProperties properties) {
        super(name, nearCacheConfig, nearCacheRecordStore, serializationService, scheduler, classLoader, properties);
        this.nearCacheManager = checkNotNull(nearCacheManager, "nearCacheManager cannot be null");
    }

    @Override
    public void initialize() {
        super.initialize();

        memoryManager = createMemoryManager();
        nearCacheRecordStore.initialize();
    }

    private HazelcastMemoryManager createMemoryManager() {
        if (nearCacheRecordStore instanceof HDNearCacheRecordStore) {
            return ((HDNearCacheRecordStore) nearCacheRecordStore).getMemoryManager();
        }
        return null;
    }

    @Override
    protected NearCacheRecordStore<K, V> createNearCacheRecordStore(String name, NearCacheConfig nearCacheConfig) {
        if (NATIVE == nearCacheConfig.getInMemoryFormat()) {
            EnterpriseSerializationService ss = (EnterpriseSerializationService) serializationService;
            return new SegmentedHDNearCacheRecordStore<>(name, nearCacheConfig, ss, classLoader);
        }

        return super.createNearCacheRecordStore(name, nearCacheConfig);
    }

    @Override
    public void put(K key, Data keyData, V value, Data valueData) {
        assert key != null : "key cannot be null";

        boolean memoryCompacted = false;
        do {
            try {
                super.put(key, keyData, value, valueData);
                break;
            } catch (NativeOutOfMemoryError error) {
                ignore(error);
            }

            if (evictRecordStores()) {
                continue;
            }

            if (memoryCompacted) {
                handleNativeOOME(key);
                break;
            }

            compactMemory();
            memoryCompacted = true;

        } while (true);
    }

    @Override
    public long tryReserveForUpdate(K key, Data keyData) {
        assert key != null : "key cannot be null";

        boolean memoryCompacted = false;
        do {
            try {
                return super.tryReserveForUpdate(key, keyData);
            } catch (NativeOutOfMemoryError error) {
                ignore(error);
            }

            if (evictRecordStores()) {
                continue;
            }

            if (memoryCompacted) {
                handleNativeOOME(key);
                break;
            }

            compactMemory();
            memoryCompacted = true;

        } while (true);

        return NOT_RESERVED;
    }

    @Override
    public V tryPublishReserved(K key, V value, long reservationId, boolean deserialize) {
        assert key != null : "key cannot be null";

        boolean memoryCompacted = false;
        do {
            try {
                return super.tryPublishReserved(key, value, reservationId, deserialize);
            } catch (NativeOutOfMemoryError error) {
                ignore(error);
            }

            if (evictRecordStores()) {
                continue;
            }

            if (memoryCompacted) {
                handleNativeOOME(key);
                break;
            }

            compactMemory();
            memoryCompacted = true;

        } while (true);

        return null;
    }

    private void handleNativeOOME(K key) {
        // there may be an existing entry in Near Cache for the specified `key`, to be in safe side, remove that entry,
        // otherwise stale value for that `key` may be seen indefinitely. This removal will make subsequent gets to fetch
        // the value from underlying IMap/cache
        super.invalidate(key);

        // due to the ongoing compaction, one user thread may not see sufficient space to put entry into Near Cache;
        // in that case, skipping NativeOutOfMemoryError instead of throwing it to user (even eviction is configured)
        // this is because Near Cache feature is an optimization and it is okay not to put some entries;
        // we are expecting to put next entries into Near Cache after compaction or after Near Cache invalidation
        if (logger.isLoggable(WARNING)) {
            logger.warning(format("Entry can not be put into Near Cache for this time: nearCacheName=%s", name));
        }
    }

    // evict a record from a record-store regardless from eviction max-size policy
    private boolean evictRecordStores() {
        // First: Try to evict this near caches record-store.
        if (evict(nearCacheRecordStore)) {
            return true;
        }

        // Second: Try to evict any other record-stores.
        Collection<NearCache> nearCacheList = nearCacheManager.listAllNearCaches();
        for (NearCache nearCache : nearCacheList) {
            if (nearCache != this && nearCache instanceof HDNearCache) {
                if (evict(((HDNearCache) nearCache).nearCacheRecordStore)) {
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean evict(NearCacheRecordStore nearCacheRecordStore) {
        if (nearCacheRecordStore.size() == 0) {
            return false;
        }

        nearCacheRecordStore.doEviction(true);
        return true;
    }

    private void compactMemory() {
        assert memoryManager != null : "memoryManager cannot be null";

        memoryManager.compact();
    }

    // just for testing
    void setMemoryManager(HazelcastMemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }
}
