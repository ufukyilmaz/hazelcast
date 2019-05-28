package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanSyncEventTest {

    @Test
    public void testConstructor() {
        new WanSyncEvent(WanSyncType.ALL_MAPS);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testConstructor_whenTypeIsNull_thenAssert() {
        new WanSyncEvent(null);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testConstructor_whenTypeIsNotAllMaps_thenAssert() {
        new WanSyncEvent(WanSyncType.SINGLE_MAP);
    }

    @Test
    public void testSerialization() {
        Set<Integer> partitionIds = new HashSet<Integer>();
        partitionIds.add(23);
        partitionIds.add(42);

        WanSyncEvent expected = new WanSyncEvent(WanSyncType.ALL_MAPS, "eventName");
        expected.setPartitionSet(partitionIds);
        expected.setOp(new WanAntiEntropyEventPublishOperation());

        EnterpriseSerializationService serializationService = new EnterpriseSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(expected);
        WanSyncEvent deserialized = serializationService.toObject(serialized, WanSyncEvent.class);

        assertEquals(expected.getType(), deserialized.getType());
        assertEquals(expected.getMapName(), deserialized.getMapName());
        assertEquals(expected.getPartitionSet(), deserialized.getPartitionSet());
        assertNull(deserialized.getOp());
    }
}
