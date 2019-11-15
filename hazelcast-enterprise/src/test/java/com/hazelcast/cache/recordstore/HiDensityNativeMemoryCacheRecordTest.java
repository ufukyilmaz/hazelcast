package com.hazelcast.cache.recordstore;

import com.hazelcast.cache.impl.hidensity.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.UnsafeMalloc;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNativeMemoryCacheRecordTest {

    private static final LibMalloc MALLOC = new UnsafeMalloc();

    private HiDensityNativeMemoryCacheRecord record;
    private long address;

    @Before
    public void setUp() {
        address = MALLOC.malloc(HiDensityNativeMemoryCacheRecord.SIZE);
        record = new HiDensityNativeMemoryCacheRecord(address);
        record.zero();
    }

    @After
    public void tearDown() {
        MALLOC.free(address);
    }

    @Test
    public void testFields() {
        long creationTime = System.currentTimeMillis();
        record.setCreationTime(creationTime);

        long accessTime = creationTime + TimeUnit.HOURS.toMillis(10);
        record.setLastAccessTime(accessTime);

        int hit = (int) (Math.random() * 1000);
        record.setHits(hit);

        int seq = (int) (Math.random() * 99999);
        record.setSequence(seq);

        long ttl = TimeUnit.DAYS.toMillis(30);
        record.setTtlMillis(ttl);

        record.setValueAddress(address);
        record.setExpiryPolicyAddress(address);

        assertEquals(creationTime, record.getCreationTime());
        assertEquals(accessTime, record.getLastAccessTime());
        assertEquals(hit, record.getHits());
        assertEquals(seq, record.getSequence());
        assertEquals(ttl, record.getTtlMillis());
        assertEquals(address, record.getValueAddress());
        assertEquals(address, record.getExpiryPolicyAddress());
    }

    @Test
    public void testTTL() {
        long creationTime = System.currentTimeMillis();
        record.setCreationTime(creationTime);

        long ttl = TimeUnit.DAYS.toMillis(30);
        record.setTtlMillis(ttl);

        long expirationTime = creationTime + ttl;
        assertEquals(expirationTime, record.getExpirationTime());

        assertTrue(record.isExpiredAt(expirationTime + 1));
    }

    @Test
    public void testExpirationTime() {
        long creationTime = System.currentTimeMillis();
        record.setCreationTime(creationTime);

        long expirationTime = creationTime + TimeUnit.DAYS.toMillis(30);
        record.setExpirationTime(expirationTime);

        long ttl = expirationTime - creationTime;
        assertEquals(ttl, record.getTtlMillis());

        assertTrue(record.isExpiredAt(expirationTime + 1));
    }

    @Test
    public void testAccessHit() {
        for (int i = 0; i < 111; i++) {
            record.incrementHits();
        }
        assertEquals(111, record.getHits());
    }

    @Test
    public void testClear() {
        record.setCreationTime(System.currentTimeMillis());
        record.setLastAccessTime(System.currentTimeMillis());
        record.setHits(1234);
        record.setSequence(123456789L);
        record.setTtlMillis(System.currentTimeMillis());

        record.clear();

        for (int i = 0; i < HiDensityNativeMemoryCacheRecord.SIZE; i++) {
            assertEquals(0, record.readByte(i));
        }
    }
}
