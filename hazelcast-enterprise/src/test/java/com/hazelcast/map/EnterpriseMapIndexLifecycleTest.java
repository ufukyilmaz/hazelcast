package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.spi.properties.ClusterProperty.GLOBAL_HD_INDEX_ENABLED;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseMapIndexLifecycleTest extends MapIndexLifecycleTest {

    @Parameterized.Parameters(name = "format:{0} globalIndex:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {BINARY, "true"},
                {OBJECT, "true"},
                {NATIVE, "true"},
                {NATIVE, "false"},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public String globalIndex;

    @Override
    protected Config getConfig() {
        if (inMemoryFormat == NATIVE) {
            Config config = getHDConfig(super.getConfig());
            config.getNativeMemoryConfig().setAllocatorType(POOLED);
            config.setProperty(GLOBAL_HD_INDEX_ENABLED.getName(), globalIndex);
            return config;
        } else {
            return super.getConfig();
        }
    }

    @Override
    boolean globalIndex() {
        return globalIndex.equals("true");
    }
}
