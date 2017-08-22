package com.hazelcast.client.cache.nearcache;

import com.hazelcast.client.cache.impl.nearcache.ClientNearCacheInvalidationTest;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.cache.hotrestart.HotRestartTestUtil.createFolder;
import static com.hazelcast.cache.hotrestart.HotRestartTestUtil.getBaseDir;
import static com.hazelcast.cache.hotrestart.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.nio.IOUtil.deleteQuietly;

/**
 * Test publishing of Near Cache invalidation events, when the cache is configured with NATIVE in-memory format and
 * with hot restart enabled.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({SlowTest.class})
public class HotRestartClientNearCacheInvalidationTest extends ClientNearCacheInvalidationTest {

    private static final MemorySize SERVER_NATIVE_MEMORY_SIZE = new MemorySize(16, MemoryUnit.MEGABYTES);
    private static final MemorySize CLIENT_NATIVE_MEMORY_SIZE = new MemorySize(16, MemoryUnit.MEGABYTES);

    @Parameters(name = "fromMember:{0}, format:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false, InMemoryFormat.BINARY},
                {false, InMemoryFormat.OBJECT},
                {false, InMemoryFormat.NATIVE},
                {true, InMemoryFormat.BINARY},
                {true, InMemoryFormat.OBJECT},
                {true, InMemoryFormat.NATIVE},
        });
    }

    @Rule
    public TestName testName = new TestName();

    private File folder;

    @Override
    public void setup() {
        folder = isolatedFolder(getClass(), testName);
        createFolder(folder);
        super.setup();
    }

    @Override
    public void tearDown() {
        super.tearDown();
        deleteQuietly(folder);
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");

        config.getNativeMemoryConfig()
                .setEnabled(true)
                .setSize(SERVER_NATIVE_MEMORY_SIZE);

        config.getHotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(getBaseDir(folder));

        return config;
    }

    @Override
    protected ClientConfig createClientConfig() {
        ClientConfig clientConfig = super.createClientConfig();

        clientConfig.getNativeMemoryConfig()
                .setEnabled(true)
                .setSize(CLIENT_NATIVE_MEMORY_SIZE);

        return clientConfig;
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return super.createNearCacheConfig(inMemoryFormat);
    }

    @Override
    protected CacheConfig createCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(99);

        CacheConfig cacheConfig = super.createCacheConfig(InMemoryFormat.NATIVE);
        cacheConfig.setEvictionConfig(evictionConfig);

        // enable hot restart
        HotRestartConfig hrConfig = new HotRestartConfig().setEnabled(true);
        cacheConfig.setHotRestartConfig(hrConfig);

        return cacheConfig;
    }
}
