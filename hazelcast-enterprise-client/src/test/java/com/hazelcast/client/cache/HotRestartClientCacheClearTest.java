package com.hazelcast.client.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.client.HotRestartTestUtil.createFolder;
import static com.hazelcast.client.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.client.HotRestartTestUtil.getBaseDir;
import static com.hazelcast.nio.IOUtil.deleteQuietly;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClientCacheClearTest extends ClientCacheClearTest {

    @Rule
    public TestName testName = new TestName();

    private File folder;

    @Override
    protected void onSetup() {
        folder = isolatedFolder(getClass(), testName);
        createFolder(folder);
        super.onSetup();
    }

    @Override
    protected void onTearDown() {
        super.onTearDown();
        deleteQuietly(folder);
    }

    @Override
    protected Config createConfig() {
        Config config = super.getConfig();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SampleLicense.UNLIMITED_LICENSE);
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100");

        // to reduce used native memory size
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");

        config.getHotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(getBaseDir(folder));

        config.getNativeMemoryConfig().setEnabled(true)
                .setSize(new MemorySize(128, MemoryUnit.MEGABYTES))
                .setMetadataSpacePercentage(20);

        return config;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(15);

        HotRestartConfig hrConfig = new HotRestartConfig().setEnabled(true);

        CacheConfig<K, V> cacheConfig = super.createCacheConfig();
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        cacheConfig.setEvictionConfig(evictionConfig);
        cacheConfig.setHotRestartConfig(hrConfig);

        return cacheConfig;
    }
}
