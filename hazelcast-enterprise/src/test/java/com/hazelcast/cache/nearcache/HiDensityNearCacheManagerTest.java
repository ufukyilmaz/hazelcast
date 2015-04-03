package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheManager;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNearCacheManagerTest extends NearCacheManagerTestSupport {

    @Override
    protected NearCacheManager createNearCacheManager() {
        return new HiDensityNearCacheManager();
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

}
