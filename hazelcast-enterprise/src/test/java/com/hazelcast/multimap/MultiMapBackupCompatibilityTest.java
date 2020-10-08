package com.hazelcast.multimap;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class MultiMapBackupCompatibilityTest extends MultiMapBackupTest {

    @Test
    @Override
    @Ignore
    public void testBackupsPutAll() {
        // RU_COMPAT_4_0 : putAllAsync is introduced in 4.1
    }
}
