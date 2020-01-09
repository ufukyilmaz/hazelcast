package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getSmallInstanceHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDIndexConcurrencyTest extends AbstractIndexConcurrencyTest {

    @Override
    protected Config getConfig() {
        return getSmallInstanceHDConfig();
    }
}
