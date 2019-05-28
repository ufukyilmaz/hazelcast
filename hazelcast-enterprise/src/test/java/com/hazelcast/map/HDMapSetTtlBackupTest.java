package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
public class HDMapSetTtlBackupTest extends MapSetTtlBackupTest {

    @Parameters(name = "inMemoryFormat:{0}")
    public static Object[] memoryFormat() {
        return new Object[]{
                InMemoryFormat.BINARY,
                InMemoryFormat.OBJECT,
                InMemoryFormat.NATIVE,
        };
    }

    @Override
    protected Config getConfig() {
        return getHDConfig(super.getConfig(), STANDARD, new MemorySize(128, MemoryUnit.MEGABYTES));
    }
}
