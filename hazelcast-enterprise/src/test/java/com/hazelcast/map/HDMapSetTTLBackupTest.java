package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category({QuickTest.class, ParallelTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
public class HDMapSetTTLBackupTest extends MapSetTTLBackupTest {

    @Parameterized.Parameters(name = "inMemoryFormat: {0}")
    public static Object[] memoryFormat() {
        return new Object[] {InMemoryFormat.BINARY, InMemoryFormat.OBJECT, InMemoryFormat.NATIVE};
    }

    protected Config getConfig() {
        Config config = new Config();
        config.getNativeMemoryConfig().setEnabled(true).setSize(new MemorySize(128, MemoryUnit.MEGABYTES));
        return config;
    }
}
