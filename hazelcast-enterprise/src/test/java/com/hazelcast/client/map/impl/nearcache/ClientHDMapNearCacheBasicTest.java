package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static com.hazelcast.internal.memory.impl.PersistentMemoryHeap.PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY;
import static com.hazelcast.internal.nearcache.HiDensityNearCacheTestUtils.createNativeMemoryConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.util.OsHelper.isLinux;
import static java.util.Arrays.asList;

/**
 * Basic HiDensity Near Cache tests for {@link IMap} on Hazelcast clients.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientHDMapNearCacheBasicTest extends ClientMapNearCacheBasicTest {

    @Parameterized.Parameter
    public String persistentMemoryDirectory;

    @Parameterized.Parameters(name = "persistentMemoryDirectory: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {PERSISTENT_MEMORY_DIRECTORY},
                {null},
        });
    }

    @Override
    protected Config getConfig() {
        return getHDConfig(persistentMemoryDirectory);
    }

    @Before
    @Override
    public void setUp() {
        if (persistentMemoryDirectory != null) {
            Assume.assumeTrue("Only Linux platform supported", isLinux());
        }
        nearCacheConfig = createNearCacheConfig(NATIVE, DEFAULT_SERIALIZE_KEYS);
    }

    @BeforeClass
    public static void init() {
        System.setProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY, "true");
    }

    @AfterClass
    public static void cleanup() {
        System.clearProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY);
    }

    @Override
    protected ClientConfig getClientConfig() {
        NativeMemoryConfig memoryConfig = createNativeMemoryConfig();
        memoryConfig.setPersistentMemoryDirectory(persistentMemoryDirectory);
        return super.getClientConfig()
                .setNativeMemoryConfig(memoryConfig);
    }
}
