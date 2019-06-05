package com.hazelcast.cache;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class EnterpriseCacheCreationTest extends CacheCreationTest {
    @Test
    public void createInvalidWanReplicatedCache_fromProgrammaticConfig_throwsException_fromCacheManager_getCache() {
        Config config = createInvalidWanReplicatedCacheConfig();
        CachingProvider cachingProvider = createCachingProvider(config);
        CacheManager defaultCacheManager = cachingProvider.getCacheManager();

        thrown.expect(InvalidConfigurationException.class);
        defaultCacheManager.getCache("test");
    }

    private Config createInvalidWanReplicatedCacheConfig() {
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig().setName("test");
        cacheSimpleConfig.setWanReplicationRef(new WanReplicationRef().setName("missingWanConfig"));

        return createBasicConfig().addCacheConfig(cacheSimpleConfig);
    }
}
