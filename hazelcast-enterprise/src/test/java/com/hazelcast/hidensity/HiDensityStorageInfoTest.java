package com.hazelcast.hidensity;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityStorageInfoTest {

    @Test
    public void hiDensityStorageInfoInformationsRetrievedSuccessfully() {
        HiDensityStorageInfo info = new HiDensityStorageInfo("myStroage");
        assertEquals("myStroage", info.getStorageName());
        assertEquals(0, info.getEntryCount());
        assertEquals(10, info.addEntryCount(10));
        assertEquals("myStroage".hashCode(), info.hashCode());
        HiDensityStorageInfo info2 = new HiDensityStorageInfo("myStroage");
        assertEquals(info, info2);
    }

}

