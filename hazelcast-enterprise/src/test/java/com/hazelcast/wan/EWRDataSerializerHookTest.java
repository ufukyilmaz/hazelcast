package com.hazelcast.wan;

import com.hazelcast.cache.wan.CacheReplicationRemove;
import com.hazelcast.cache.wan.CacheReplicationUpdate;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link com.hazelcast.enterprise.wan.EWRDataSerializerHook}
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
@Ignore
public class EWRDataSerializerHookTest {

    @Test
    public void testExistingTypes() {
        EWRDataSerializerHook hook = new EWRDataSerializerHook();
        IdentifiedDataSerializable batchWanRep = hook.createFactory().create(EWRDataSerializerHook.BATCH_WAN_REP_EVENT);
        assertTrue(batchWanRep instanceof BatchWanReplicationEvent);

        IdentifiedDataSerializable cacheUpdate = hook.createFactory().create(EWRDataSerializerHook.CACHE_REPLICATION_UPDATE);
        assertTrue(cacheUpdate instanceof CacheReplicationUpdate);

        IdentifiedDataSerializable cacheRemove = hook.createFactory().create(EWRDataSerializerHook.CACHE_REPLICATION_REMOVE);
        assertTrue(cacheRemove instanceof CacheReplicationRemove);

        IdentifiedDataSerializable mapUpdate = hook.createFactory().create(EWRDataSerializerHook.MAP_REPLICATION_UPDATE);
        assertTrue(mapUpdate instanceof EnterpriseMapReplicationUpdate);

        IdentifiedDataSerializable mapRemove = hook.createFactory().create(EWRDataSerializerHook.MAP_REPLICATION_REMOVE);
        assertTrue(mapRemove instanceof EnterpriseMapReplicationRemove);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidType() {
        EWRDataSerializerHook hook = new EWRDataSerializerHook();
        hook.createFactory().create(999);
    }

}
