package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getSmallInstanceHDConfig;
import static com.hazelcast.config.IndexType.SORTED;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDIndexConcurrencyTest extends AbstractIndexConcurrencyTest {

    @Parameterized.Parameters(name = "indexAttribute: {0}, indexType: {1}")
    public static Collection<Object[]> parameters() {
        // @formatter:off
        return asList(new Object[][]{
                {"age", SORTED},
                {"name", SORTED}
        });
        // @formatter:on
    }

    @Override
    protected Config getConfig() {
        return getSmallInstanceHDConfig();
    }
}
