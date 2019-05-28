package com.hazelcast.map.impl.record;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.TimeUtil.zeroOutMs;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDRecordTest extends HazelcastTestSupport {

    private HDRecord record;
    private PoolingMemoryManager memoryManager;

    @Before
    public void setup() {
        MemorySize memorySize = new MemorySize(4, MemoryUnit.MEGABYTES);
        memoryManager = new PoolingMemoryManager(memorySize);
        memoryManager.registerThread(Thread.currentThread());

        record = new HDRecord(null);
        record.reset(memoryManager.allocate(HDRecord.SIZE));
    }

    @After
    public void tearDown() {
        memoryManager.free(record.address(), HDRecord.SIZE);
        memoryManager.dispose();
    }

    @Test
    public void testRecordDataCorrectness() {
        long creationTime = zeroOutMs(Clock.currentTimeMillis());
        long lastUpdatedTime = creationTime + 10000;
        long lastAccessTime = creationTime + 20000;
        long lastStoredTime = creationTime + 30000;

        long expirationTime = creationTime + 500000;

        long hits = 1000;
        long ttl = 10000;
        long maxIdle = 5000;

        record.setCreationTime(creationTime);
        record.setLastUpdateTime(lastUpdatedTime);
        record.setLastAccessTime(lastAccessTime);
        record.setLastStoredTime(lastStoredTime);
        record.setHits(hits);
        record.setExpirationTime(expirationTime);
        record.setTtl(ttl);
        record.setMaxIdle(maxIdle);
        // Setting sequence (last field on the allocated chunk) can cause a segfault, if the HDRecord.SIZE (used during alloc)
        // is not enough to fit all data.
        record.setSequence(1);

        assertEquals(creationTime, record.getCreationTime());
        assertEquals(lastUpdatedTime, record.getLastUpdateTime());
        assertEquals(lastAccessTime, record.getLastAccessTime());
        assertEquals(lastStoredTime, record.getLastStoredTime());
        assertEquals(hits, record.getHits());
        assertEquals(expirationTime, record.getExpirationTime());
        assertEquals(ttl, record.getTtl());
        assertEquals(maxIdle, record.getMaxIdle());
    }

    @Test
    public void testTTL_MaxIdle_ExpirationTime_limits() {
        record.setExpirationTime(Long.MAX_VALUE);
        record.setTtl(Long.MAX_VALUE);
        record.setMaxIdle(Long.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, record.getMaxIdle());
        assertEquals(Long.MAX_VALUE, record.getTtl());
        assertEquals(Long.MAX_VALUE, record.getExpirationTime());
    }

    @Test
    public void testBaseTime_whenNegativeOffset() {
        long baseTime = AbstractRecord.EPOCH_TIME;
        long creationTime = baseTime - randomLong();
        long expirationTime = baseTime + randomLong();
        record.setCreationTime(creationTime);
        record.setExpirationTime(expirationTime);

        assertEquals(zeroOutMs(creationTime), record.getCreationTime());
        assertEquals(zeroOutMs(expirationTime), record.getExpirationTime());
    }

    private long randomLong() {
        return (long) Math.random() * 50000L;
    }

}
