package com.hazelcast.internal.nearcache;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDNearCacheManagerTest extends NearCacheManagerTestSupport {

    @Override
    protected NearCacheManager createNearCacheManager() {
        return new HDNearCacheManager(((EnterpriseSerializationService) ss),
                executionService.getGlobalTaskScheduler(),
                null, properties);
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
