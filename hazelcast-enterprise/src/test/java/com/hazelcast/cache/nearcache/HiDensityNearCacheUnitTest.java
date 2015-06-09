package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.hidensity.operation.CacheContainsKeyOperation;
import com.hazelcast.cache.hidensity.operation.CacheGetAndReplaceOperation;
import com.hazelcast.cache.hidensity.operation.CacheKeyIteratorOperation;
import com.hazelcast.cache.hidensity.operation.CacheLoadAllOperationFactory;
import com.hazelcast.cache.hidensity.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.hidensity.operation.CacheReplaceOperation;
import com.hazelcast.cache.hidensity.operation.CacheSizeOperation;
import com.hazelcast.cache.hidensity.operation.HiDensityCacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.operation.CacheEntryProcessorOperation;
import com.hazelcast.cache.impl.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheSizeOperationFactory;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNearCacheUnitTest {

    @Test
    public void HiDensityStorageInfoTest() throws Exception {
        HiDensityStorageInfo info = new HiDensityStorageInfo("myStroage");
        assertEquals("myStroage", info.getStorageName());
        assertEquals(0, info.getEntryCount());
        assertEquals(10, info.addEntryCount(10));
        assertEquals("myStroage".hashCode(), info.hashCode());
        HiDensityStorageInfo info2 = new HiDensityStorageInfo("myStroage");
        assertEquals(info, info2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void HiDensityCacheDataSerializerHookUnitTest() throws Exception {

        HiDensityCacheDataSerializerHook hook = new HiDensityCacheDataSerializerHook();
        DataSerializableFactory factory = hook.createFactory();

        assertTrue(factory.create(HiDensityCacheDataSerializerHook.CONTAINS_KEY)
                instanceof CacheContainsKeyOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.PUT_IF_ABSENT)
                instanceof CachePutIfAbsentOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.GET_AND_REMOVE)
                instanceof CacheGetAndRemoveOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.REPLACE)
                instanceof CacheReplaceOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.GET_AND_REPLACE)
                instanceof CacheGetAndReplaceOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.SIZE)
                instanceof CacheSizeOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.SIZE_FACTORY)
                instanceof CacheSizeOperationFactory);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.ITERATE)
                instanceof CacheKeyIteratorOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.ITERATION_RESULT)
                instanceof CacheKeyIteratorResult);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.LOAD_ALL_FACTORY)
                instanceof CacheLoadAllOperationFactory);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.ENTRY_PROCESSOR)
                instanceof CacheEntryProcessorOperation);
        factory.create(Integer.MIN_VALUE);

    }
}
