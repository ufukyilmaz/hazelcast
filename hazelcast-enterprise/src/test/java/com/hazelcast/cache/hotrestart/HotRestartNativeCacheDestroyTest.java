package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.CacheClearTest;
import com.hazelcast.cache.CacheDestroyTest;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class})
public class HotRestartNativeCacheDestroyTest
        extends CacheDestroyTest {

    private File folder;

    @Rule
    public TestName testName = new TestName();

    @Override
    protected Config createConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SampleLicense.UNLIMITED_LICENSE);
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100");

        // to reduce used native memory size
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");

        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        hotRestartPersistenceConfig.setEnabled(true);
        hotRestartPersistenceConfig.setBaseDir(folder);

        config.getNativeMemoryConfig().setEnabled(true)
              .setSize(new MemorySize(128, MemoryUnit.MEGABYTES))
              .setMetadataSpacePercentage(20);
        return config;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig<K, V> cacheConfig = super.createCacheConfig();
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(20);
        cacheConfig.setEvictionConfig(evictionConfig);
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        HotRestartConfig hrConfig = new HotRestartConfig().setEnabled(true);
        cacheConfig.setHotRestartConfig(hrConfig);
        return cacheConfig;
    }

    @Override
    protected void onSetup() {
        folder = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(folder);
        if (!folder.mkdir() && !folder.exists()) {
            throw new AssertionError("Unable to create test folder: " + folder.getAbsolutePath());
        }

        super.onSetup();
    }

    @Override
    protected void onTearDown() {
        super.onTearDown();
        if (folder != null) {
            delete(folder);
        }
    }

}
