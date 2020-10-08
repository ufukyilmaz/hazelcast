package com.hazelcast.collection.impl.queue;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class QueueMigrationCompatibilityTest extends QueueMigrationTest {

    @Test
    @Ignore
    @Override
    public void testMigration() {
        // RU_COMPAT_4_0 : uses VersionedObject, new in 4.1
    }

    @Test
    @Ignore
    @Override
    public void testPromotion() {
        // RU_COMPAT_4_0 : uses VersionedObject, new in 4.1
    }


}
