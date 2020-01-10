package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.internal.nearcache.impl.HDNearCacheTestUtils.createNativeMemoryConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ClientHDCacheNearCacheStaleReadTest extends ClientCacheNearCacheStaleReadTest {

    @Override
    protected CacheConfig<String, String> createCacheConfig(String cacheName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return super.createCacheConfig(cacheName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }

    @Override
    protected Config getConfig() {
        return getHDConfig(super.getConfig());
    }

    @Override
    protected ClientConfig getClientConfig(String cacheName) {
        NativeMemoryConfig nativeMemoryConfig = createNativeMemoryConfig();

        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        NearCacheConfig nearCacheConfig = getNearCacheConfig(cacheName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);

        return super.getClientConfig(cacheName)
                .setNativeMemoryConfig(nativeMemoryConfig)
                .addNearCacheConfig(nearCacheConfig);
    }
}
