package com.hazelcast.map.impl.record;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.TimeUtil.zeroOutMs;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDRecordTest extends HazelcastTestSupport {

    private HDRecordWithStats record;
    private PoolingMemoryManager memoryManager;

    @Before
    public void setup() {
        MemorySize memorySize = new MemorySize(5, MemoryUnit.MEGABYTES);
        memoryManager = new PoolingMemoryManager(memorySize);
        memoryManager.registerThread(Thread.currentThread());

        record = new HDRecordWithStats();
        record.reset(memoryManager.allocate(HDRecordWithStats.SIZE));
    }

    @After
    public void tearDown() {
        memoryManager.free(record.address(), HDRecordWithStats.SIZE);
        memoryManager.dispose();
    }

    @Test
    public void testRecordDataCorrectness() {
        long creationTime = zeroOutMs(Clock.currentTimeMillis());
        long lastUpdatedTime = creationTime + 10000;
        long lastAccessTime = creationTime + 20000;
        long lastStoredTime = creationTime + 30000;

        long expirationTime = creationTime + 500000;

        int hits = 1000;
        long ttl = 10000;
        long maxIdle = 5000;

        record.setCreationTime(creationTime);
        record.setLastUpdateTime(lastUpdatedTime);
        record.setLastAccessTime(lastAccessTime);
        record.setLastStoredTime(lastStoredTime);
        record.setHits(hits);
        // Setting sequence (last field on the allocated chunk) can cause a segfault, if the HDRecord.SIZE (used during alloc)
        // is not enough to fit all data.
        record.setSequence(1);

        assertEquals(creationTime, record.getCreationTime());
        assertEquals(lastUpdatedTime, record.getLastUpdateTime());
        assertEquals(lastAccessTime, record.getLastAccessTime());
        assertEquals(lastStoredTime, record.getLastStoredTime());
        assertEquals(hits, record.getHits());
    }

    @Test
    public void testBaseTime_whenNegativeOffset() {
        long baseTime = AbstractRecord.EPOCH_TIME;
        long creationTime = baseTime - randomLong();
        long expirationTime = baseTime + randomLong();
        record.setCreationTime(creationTime);

        assertEquals(zeroOutMs(creationTime), record.getCreationTime());
    }

    private long randomLong() {
        return (long) Math.random() * 50000L;
    }

}
