package com.hazelcast.cache.operation;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link com.hazelcast.cache.operation.EnterpriseCacheDataSerializerHook}
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseCacheDataSerializerHookTest {

    @Test
    public void testExistingTypes() {
        EnterpriseCacheDataSerializerHook hook = new EnterpriseCacheDataSerializerHook();
        IdentifiedDataSerializable merge = hook.createFactory().create(EnterpriseCacheDataSerializerHook.WAN_MERGE);
        assertTrue(merge instanceof WanCacheMergeOperation);
        IdentifiedDataSerializable remove = hook.createFactory().create(EnterpriseCacheDataSerializerHook.WAN_REMOVE);
        assertTrue(remove instanceof WanCacheRemoveOperation);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidType() {
        EnterpriseCacheDataSerializerHook hook = new EnterpriseCacheDataSerializerHook();
        hook.createFactory().create(999);
    }

}
