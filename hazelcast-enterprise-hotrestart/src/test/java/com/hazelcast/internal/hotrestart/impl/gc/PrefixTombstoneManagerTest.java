package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.collection.Long2LongHashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PrefixTombstoneManagerTest {

    @Test
    public void testGetMaxChunkSeq() {
        final Long2LongHashMap map = new Long2LongHashMap(0);
        map.put(1, 5);
        map.put(2, 10);
        map.put(3, 4);
        final PrefixTombstoneManager manager = new PrefixTombstoneManager(null, null, null, null);
        manager.setPrefixTombstones(map);
        assertEquals((long) Collections.max(map.values()), manager.maxRecordSeq());
    }
}
