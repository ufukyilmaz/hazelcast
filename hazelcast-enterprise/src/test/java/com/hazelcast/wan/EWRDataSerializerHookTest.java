package com.hazelcast.wan;

import com.hazelcast.cache.wan.CacheReplicationRemove;
import com.hazelcast.cache.wan.CacheReplicationUpdate;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.operation.PostJoinWanOperation;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEventPublishOperation;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.enterprise.wan.EWRDataSerializerHook.BATCH_WAN_REP_EVENT;
import static com.hazelcast.enterprise.wan.EWRDataSerializerHook.CACHE_REPLICATION_REMOVE;
import static com.hazelcast.enterprise.wan.EWRDataSerializerHook.CACHE_REPLICATION_UPDATE;
import static com.hazelcast.enterprise.wan.EWRDataSerializerHook.MAP_REPLICATION_REMOVE;
import static com.hazelcast.enterprise.wan.EWRDataSerializerHook.MAP_REPLICATION_UPDATE;
import static com.hazelcast.enterprise.wan.EWRDataSerializerHook.POST_JOIN_WAN_OPERATION;
import static com.hazelcast.enterprise.wan.EWRDataSerializerHook.WAN_SYNC_OPERATION;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link com.hazelcast.enterprise.wan.EWRDataSerializerHook}
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EWRDataSerializerHookTest {

    @Test
    public void testExistingTypes() {
        EWRDataSerializerHook hook = new EWRDataSerializerHook();
        IdentifiedDataSerializable batchWanRep = hook.createFactory().create(BATCH_WAN_REP_EVENT);
        assertTrue(batchWanRep instanceof BatchWanReplicationEvent);

        IdentifiedDataSerializable cacheUpdate = hook.createFactory().create(CACHE_REPLICATION_UPDATE);
        assertTrue(cacheUpdate instanceof CacheReplicationUpdate);

        IdentifiedDataSerializable cacheRemove = hook.createFactory().create(CACHE_REPLICATION_REMOVE);
        assertTrue(cacheRemove instanceof CacheReplicationRemove);

        IdentifiedDataSerializable mapUpdate = hook.createFactory().create(MAP_REPLICATION_UPDATE);
        assertTrue(mapUpdate instanceof EnterpriseMapReplicationUpdate);

        IdentifiedDataSerializable mapRemove = hook.createFactory().create(MAP_REPLICATION_REMOVE);
        assertTrue(mapRemove instanceof EnterpriseMapReplicationRemove);

        IdentifiedDataSerializable wanSyncOperation = hook.createFactory().create(WAN_SYNC_OPERATION);
        assertTrue(wanSyncOperation instanceof WanAntiEntropyEventPublishOperation);

        IdentifiedDataSerializable postJoinWanOperation = hook.createFactory().create(POST_JOIN_WAN_OPERATION);
        assertTrue(postJoinWanOperation instanceof PostJoinWanOperation);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidType() {
        EWRDataSerializerHook hook = new EWRDataSerializerHook();
        hook.createFactory().create(999);
    }
}
