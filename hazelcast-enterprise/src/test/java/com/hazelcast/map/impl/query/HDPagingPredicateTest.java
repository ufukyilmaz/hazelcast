package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.HDTestSupport;
import com.hazelcast.map.PagingPredicateTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDPagingPredicateTest extends PagingPredicateTest {

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig();
    }
}
