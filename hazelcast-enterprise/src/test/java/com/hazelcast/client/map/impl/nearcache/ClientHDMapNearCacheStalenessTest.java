package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.internal.nearcache.HDNearCacheTestUtils.createNativeMemoryConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientHDMapNearCacheStalenessTest extends ClientMapNearCacheStalenessTest {

    @Override
    protected Config getConfig() {
        return getHDConfig(super.getConfig());
    }

    @Override
    protected ClientConfig getClientConfig(String mapName) {
        NativeMemoryConfig memoryConfig = createNativeMemoryConfig();

        return super.getClientConfig(mapName)
                .setNativeMemoryConfig(memoryConfig);
    }

    @Override
    protected NearCacheConfig getNearCacheConfig(String mapName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return super.getNearCacheConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }
}
