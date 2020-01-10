package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.internal.nearcache.NearCache.UpdateSemantic.READ_UPDATE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static org.junit.Assert.assertTrue;

/**
 * Uses IMap and Near Cache internals to stress Near Cache with native memory.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class ClientHDMapNearCacheStressTest extends HazelcastTestSupport {

    private static final String MAP_NAME = ClientHDMapNearCacheStressTest.class.getName();
    private static final long NEAR_CACHE_NATIVE_MEMORY_MEGABYTES = 64;
    private static final int NEAR_CACHE_PUT_COUNT = 5000000;
    private static final long TEST_TIMEOUT_TEN_MINUTES = 10 * 60 * 1000;

    private TestHazelcastFactory factory = new TestHazelcastFactory();
    private IMap<Object, Object> clientMap;

    @Before
    public void setUp() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = newClientConfig()
                .addNearCacheConfig(newNearCacheConfig());

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        clientMap = client.getMap(MAP_NAME);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test(timeout = TEST_TIMEOUT_TEN_MINUTES)
    public void near_cache_does_not_throw_native_oome() {
        NearCachedClientMapProxy<Object, Object> proxy = (NearCachedClientMapProxy<Object, Object>) this.clientMap;
        SerializationService serializationService = proxy.getContext().getSerializationService();
        NearCache<Object, Object> nearCache = proxy.getNearCache();

        for (int i = 0; i < NEAR_CACHE_PUT_COUNT; i++) {
            Data data = serializationService.toData(i);
            long reservationId = nearCache.tryReserveForUpdate(data, data, READ_UPDATE);
            if (reservationId != NOT_RESERVED) {
                try {
                    nearCache.tryPublishReserved(data, data, reservationId, true);
                } catch (Throwable throwable) {
                    nearCache.invalidate(data);
                    throw rethrow(throwable);
                }
            }
        }

        assertTrue(nearCache.size() > 0);
    }

    private static NearCacheConfig newNearCacheConfig() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(LRU)
                .setMaxSizePolicy(FREE_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return new NearCacheConfig(MAP_NAME)
                .setInMemoryFormat(NATIVE)
                .setEvictionConfig(evictionConfig);
    }

    private static ClientConfig newClientConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(NEAR_CACHE_NATIVE_MEMORY_MEGABYTES, MEGABYTES))
                .setAllocatorType(POOLED).setMetadataSpacePercentage(40);

        return new ClientConfig()
                .setNativeMemoryConfig(nativeMemoryConfig);
    }
}
