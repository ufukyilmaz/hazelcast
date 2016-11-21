package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterHotRestartStatusDTOUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClusterHotRestartStatusDTOUtil.class);
    }
}
