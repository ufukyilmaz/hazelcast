package com.hazelcast.client.cache.nearcache.invalidation;

import com.hazelcast.client.cache.impl.nearcache.invalidation.ClientCacheMetaDataGeneratorTest;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientHDCacheMetaDataGeneratorTest extends ClientCacheMetaDataGeneratorTest {

    private static final MemorySize CLIENT_NATIVE_MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Override
    protected Config getConfig() {
        return getHDConfig(super.getConfig());
    }

    @Override
    protected ClientConfig getClientConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(CLIENT_NATIVE_MEMORY_SIZE);

        return super.getClientConfig()
                .setNativeMemoryConfig(nativeMemoryConfig);
    }

    @Override
    protected CacheConfig<Integer, Integer> getCacheConfig(String cacheName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return super.getCacheConfig(cacheName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }

    @Override
    protected NearCacheConfig getNearCacheConfig() {
        return super.getNearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE);
    }
}
