package com.hazelcast.map.impl.record;


import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.hidensity.HiDensityRecordAccessor;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class CachedHDRecordWithStatsTest {
    @Test
    public void givenCachedValueIsNull_whenCASExpectNull_thenNewValueIsSet() {
        //given
        HiDensityRecordAccessor<HDRecord> recordAccessor = mock(HiDensityRecordAccessor.class);
        CachedHDRecordWithStats record = new CachedHDRecordWithStats(recordAccessor);

        //when
        Object expectedValue = new Object();
        record.casCachedValue(null, expectedValue);

        //then
        Object actualValue = record.getCachedValueUnsafe();
        assertSame(expectedValue, actualValue);
    }

    @Test
    public void givenCachedValueIsNotNull_whenCASExpectNull_thenNewValueIsNotSet() {
        //given
        HiDensityRecordAccessor<HDRecord> recordAccessor = mock(HiDensityRecordAccessor.class);
        CachedHDRecordWithStats record = new CachedHDRecordWithStats(recordAccessor);
        Object originalValue = new Object();
        record.casCachedValue(null, originalValue);

        //when
        Object expectedValue = new Object();
        record.casCachedValue(null, expectedValue);

        //then
        Object actualValue = record.getCachedValueUnsafe();
        assertSame(originalValue, actualValue);
    }
}
