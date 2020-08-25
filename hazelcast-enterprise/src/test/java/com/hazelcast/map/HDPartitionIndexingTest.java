package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.query.impl.PartitionIndexingTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.spi.properties.ClusterProperty.GLOBAL_HD_INDEX_ENABLED;
import static java.util.Arrays.asList;

import org.junit.runners.Parameterized.UseParametersRunnerFactory;


@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDPartitionIndexingTest extends PartitionIndexingTest {

    @Parameterized.Parameter(1)
    public String globalIndex;

    @Parameterized.Parameters(name = "format:{0} globalIndex:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, "true"},
                {InMemoryFormat.NATIVE, "false"},

        });
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.getNativeMemoryConfig().setEnabled(true).setAllocatorType(POOLED);
        config.setProperty(GLOBAL_HD_INDEX_ENABLED.getName(), globalIndex);
        return config;
    }
}
