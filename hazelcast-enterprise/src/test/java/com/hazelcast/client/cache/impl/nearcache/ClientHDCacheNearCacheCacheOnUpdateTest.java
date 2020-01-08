package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.nearcache.HDNearCache;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nearcache.HDNearCacheTestUtils.createNativeMemoryConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientHDCacheNearCacheCacheOnUpdateTest extends ClientCacheNearCacheCacheOnUpdateTest {

    @Override
    protected InMemoryFormat nearCacheInMemoryFormat() {
        return InMemoryFormat.NATIVE;
    }

    @Override
    protected ClientConfig getClientConfig() {
        return super.getClientConfig()
                .setNativeMemoryConfig(createNativeMemoryConfig());
    }

    @Override
    protected void checkNearCacheInstance(ICache iCacheOnClient) {
        NearCache nearCache = ((NearCachedClientCacheProxy<Object, Object>) iCacheOnClient).getNearCache();
        assertInstanceOf(HDNearCache.class, nearCache);
    }
}
