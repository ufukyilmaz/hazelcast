package com.hazelcast.wan;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for {@link com.hazelcast.enterprise.wan.replication.WanReplicationProperties}
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanReplicationPropertiesTest extends HazelcastTestSupport {

    @Test
    public void testPrivateConstructor() {
        assertUtilityConstructor(WanReplicationProperties.class);
    }

}
