package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.map.impl.recordstore.EnterpriseRecordStore.HD_RECORD_MAX_TTL_MILLIS;
import static com.hazelcast.util.RandomPicker.getInt;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseRecordStoreTest extends HazelcastTestSupport {

    /**
     * Threshold for NATIVE is {@link EnterpriseRecordStore#HD_RECORD_MAX_TTL_MILLIS}
     *
     * @see EnterpriseRecordStore#markRecordStoreExpirable(long, long)
     */
    @Test
    public void testHDRecord_not_mark_recordstore_expirable_when_ttl_is_higher_than_threshold() throws Exception {
        EnterpriseRecordStore recordStore = getEnterpriseRecordStore();
        recordStore.markRecordStoreExpirable(HD_RECORD_MAX_TTL_MILLIS + getInt(0, Integer.MAX_VALUE), -1);

        assertFalse("Should not be marked as expirable", recordStore.isExpirable());
    }

    /**
     * Threshold for NATIVE is {@link EnterpriseRecordStore#HD_RECORD_MAX_TTL_MILLIS}
     *
     * @see EnterpriseRecordStore#markRecordStoreExpirable(long, long)
     */
    @Test
    public void testHDRecord_mark_recordstore_expirable_when_ttl_is_lower_than_threshold() throws Exception {
        EnterpriseRecordStore recordStore = getEnterpriseRecordStore();
        recordStore.markRecordStoreExpirable(HD_RECORD_MAX_TTL_MILLIS - getInt(1, Integer.MAX_VALUE), -1);

        assertTrue("Should be marked as expirable", recordStore.isExpirable());
    }

    private EnterpriseRecordStore getEnterpriseRecordStore() {
        String mapName = "test";
        Config config = getHDConfig();
        HazelcastInstance member = createHazelcastInstance(config);
        IMap map = member.getMap(mapName);
        MapService mapService = (MapService) ((MapProxyImpl) map).getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(0);
        return (EnterpriseRecordStore) partitionContainer.getRecordStore(mapName);
    }
}
