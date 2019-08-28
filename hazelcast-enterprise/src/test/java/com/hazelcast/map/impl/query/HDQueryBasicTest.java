package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.internal.memory.impl.PersistentMemoryHeap.PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDQueryBasicTest extends QueryBasicTest {

    @Parameterized.Parameter
    public String persistentMemoryDirectory;

    @Parameterized.Parameters(name = "persistentMemoryDirectory: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {PERSISTENT_MEMORY_DIRECTORY},
                {null},
        });
    }

    @Before
    public void setup() {
        System.setProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY, "true");
    }

    @After
    public void tearDown() {
        System.clearProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY);
    }


    @Override
    protected Config getConfig() {
        return getHDConfig(persistentMemoryDirectory);
    }

}
