package com.hazelcast.cache;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class CacheBasicServerCompatibilityTest extends CacheBasicServerTest {

    @Test
    @Ignore
    @Override
    public void getButCantOperateOnCacheAfterDestroy() {
        // FIXME Expected cacheAfterDestroy to be null expected null, but was:<com.hazelcast.cache.impl.CacheProxy{...}>
        // could be an issue with a duplicate CacheManager or the missing LifeCycleListener implementation
        super.getButCantOperateOnCacheAfterDestroy();
    }

    @Test
    @Ignore
    @Override
    public void removeCacheFromOwnerCacheManagerWhenCacheIsDestroyed() {
        // FIXME Expected the cache not to be found in the CacheManager
        // could be an issue with a duplicate CacheManager or the missing LifeCycleListener implementation
        super.removeCacheFromOwnerCacheManagerWhenCacheIsDestroyed();
    }

    @Test
    @Ignore
    @Override
    public void cacheManagerOfCache_cannotBeOverwritten() throws Exception {
        // FIXME AssertionError in AbstractInternalCacheProxy.setCacheManager() due to assert instanceOf
        // this should be disabled via delegateClassloader.setDefaultAssertionStatus(false) in AbstractAnswer
        super.cacheManagerOfCache_cannotBeOverwritten();
    }

    @Test
    @Ignore
    @Override
    public void cacheManagerOfCache_cannotBeOverwrittenConcurrently() throws Exception {
        // FIXME AssertionError in AbstractInternalCacheProxy.setCacheManager() due to assert instanceOf
        // this should be disabled via delegateClassloader.setDefaultAssertionStatus(false) in AbstractAnswer
        super.cacheManagerOfCache_cannotBeOverwrittenConcurrently();
    }
}
