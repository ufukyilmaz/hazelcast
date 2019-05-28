package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.UnsafeMalloc;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.HiDensityNativeMemoryCacheRecordConstructor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HiDensityNativeMemoryCacheRecordConstructorTest {

    private static final LibMalloc MALLOC = new UnsafeMalloc();

    private HiDensityNativeMemoryCacheRecord record;
    private long address;

    @Before
    public void setup() {
        address = MALLOC.malloc(HiDensityNativeMemoryCacheRecord.SIZE);
        record = new HiDensityNativeMemoryCacheRecord(address);
        record.zero();
    }

    @After
    public void teardown() {
        MALLOC.free(address);
    }

    @Test
    public void testConstructor() {
        HiDensityNativeMemoryCacheRecordConstructor constructor
                = new HiDensityNativeMemoryCacheRecordConstructor(HiDensityNativeMemoryCacheRecord.class);
        HiDensityNativeMemoryCacheRecord clonedRecord = (HiDensityNativeMemoryCacheRecord) constructor.createNew(record);

        assertEquals(record.getValue(), clonedRecord.getValue());
        assertEquals(record.getValueAddress(), clonedRecord.getValueAddress());
        assertEquals(record.getExpiryPolicy(), clonedRecord.getExpiryPolicy());
        assertEquals(record.getExpiryPolicyAddress(), clonedRecord.getExpiryPolicyAddress());
        assertEquals(record.getSequence(), clonedRecord.getSequence());
        assertEquals(record.getCreationTime(), clonedRecord.getCreationTime());
        assertEquals(record.getExpirationTime(), clonedRecord.getExpirationTime());
        assertEquals(record.getLastAccessTime(), clonedRecord.getLastAccessTime());
        assertEquals(record.getTtlMillis(), clonedRecord.getTtlMillis());
        assertEquals(record.getAccessHit(), clonedRecord.getAccessHit());
    }
}
