package com.hazelcast.wan;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisherProperties;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for {@link WanBatchPublisherProperties}
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanBatchPublisherPropertiesTest extends HazelcastTestSupport {

    @Test
    public void testPrivateConstructor() {
        assertUtilityConstructor(WanBatchPublisherProperties.class);
    }

}
