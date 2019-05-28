package com.hazelcast.json;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapPredicateJsonTest extends MapPredicateJsonTest {

    @Parameterized.Parameters(name = "inMemoryFormat: {0}, metadataPolicy: {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][] {
                {InMemoryFormat.NATIVE, MetadataPolicy.OFF},
                {InMemoryFormat.NATIVE, MetadataPolicy.CREATE_ON_UPDATE},
        });
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        NativeMemoryConfig memoryConfig = new NativeMemoryConfig().setEnabled(true);
        config.setNativeMemoryConfig(memoryConfig);
        return config;
    }
}
