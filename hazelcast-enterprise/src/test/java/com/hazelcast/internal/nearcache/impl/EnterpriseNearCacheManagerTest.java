package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseNearCacheManagerTest extends NearCacheManagerTestSupport {

    @Override
    protected NearCacheManager createNearCacheManager() {
        return new EnterpriseNearCacheManager(((EnterpriseSerializationService) ss),
                executionService.getGlobalTaskScheduler(),
                null, properties);
    }

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig(super.getConfig());
    }

    @Test
    public void createAndGetNearCache() {
        doCreateAndGetNearCache();
    }

    @Test
    public void listNearCaches() {
        doListNearCaches();
    }

    @Test
    public void clearNearCacheAndClearAllNearCaches() {
        doClearNearCacheAndClearAllNearCaches();
    }

    @Test
    public void destroyNearCacheAndDestroyAllNearCaches() {
        doDestroyNearCacheAndDestroyAllNearCaches();
    }

    @Test
    public void createsMatchingNearCacheInstanceWithInMemoryFormat_when_Binary() {
        NearCache nearCache = createNearCache(createNearCacheManager(),
                DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.BINARY);

        assertEquals(DefaultNearCache.class, nearCache.getClass());
    }

    @Test
    public void createsMatchingNearCacheInstanceWithInMemoryFormat_when_Native() {
        NearCache nearCache = createNearCache(createNearCacheManager(),
                DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);

        assertEquals(HDNearCache.class, nearCache.getClass());
    }
}
