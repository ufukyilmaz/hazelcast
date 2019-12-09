package com.hazelcast.wan;

import com.hazelcast.cache.impl.wan.WanCacheRemoveEvent;
import com.hazelcast.cache.impl.wan.WanCacheUpdateEvent;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.enterprise.wan.impl.replication.WanEventBatch;
import com.hazelcast.enterprise.wan.impl.operation.PostJoinWanOperation;
import com.hazelcast.enterprise.wan.impl.sync.WanAntiEntropyEventPublishOperation;
import com.hazelcast.map.impl.wan.WanEnterpriseMapRemoveEvent;
import com.hazelcast.map.impl.wan.WanEnterpriseMapUpdateEvent;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook.BATCH_WAN_REP_EVENT;
import static com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook.CACHE_REPLICATION_REMOVE;
import static com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook.CACHE_REPLICATION_UPDATE;
import static com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook.MAP_REPLICATION_REMOVE;
import static com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook.MAP_REPLICATION_UPDATE;
import static com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook.POST_JOIN_WAN_OPERATION;
import static com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook.WAN_SYNC_OPERATION;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link WanDataSerializerHook}
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanDataSerializerHookTest {

    @Test
    public void testExistingTypes() {
        WanDataSerializerHook hook = new WanDataSerializerHook();
        IdentifiedDataSerializable batchWanRep = hook.createFactory().create(BATCH_WAN_REP_EVENT);
        assertTrue(batchWanRep instanceof WanEventBatch);

        IdentifiedDataSerializable cacheUpdate = hook.createFactory().create(CACHE_REPLICATION_UPDATE);
        assertTrue(cacheUpdate instanceof WanCacheUpdateEvent);

        IdentifiedDataSerializable cacheRemove = hook.createFactory().create(CACHE_REPLICATION_REMOVE);
        assertTrue(cacheRemove instanceof WanCacheRemoveEvent);

        IdentifiedDataSerializable mapUpdate = hook.createFactory().create(MAP_REPLICATION_UPDATE);
        assertTrue(mapUpdate instanceof WanEnterpriseMapUpdateEvent);

        IdentifiedDataSerializable mapRemove = hook.createFactory().create(MAP_REPLICATION_REMOVE);
        assertTrue(mapRemove instanceof WanEnterpriseMapRemoveEvent);

        IdentifiedDataSerializable wanSyncOperation = hook.createFactory().create(WAN_SYNC_OPERATION);
        assertTrue(wanSyncOperation instanceof WanAntiEntropyEventPublishOperation);

        IdentifiedDataSerializable postJoinWanOperation = hook.createFactory().create(POST_JOIN_WAN_OPERATION);
        assertTrue(postJoinWanOperation instanceof PostJoinWanOperation);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidType() {
        WanDataSerializerHook hook = new WanDataSerializerHook();
        hook.createFactory().create(999);
    }
}
