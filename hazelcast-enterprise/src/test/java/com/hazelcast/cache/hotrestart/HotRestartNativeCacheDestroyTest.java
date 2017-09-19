package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.CacheDestroyTest;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.HotRestartConfig;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class})
public class HotRestartNativeCacheDestroyTest extends CacheDestroyTest {

    private static final AtomicInteger instanceIndex = new AtomicInteger();

    private File folder;

    @Rule
    public TestName testName = new TestName();

    @Override
    protected Config createConfig() {
        Config config = new Config()
                .setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SampleLicense.UNLIMITED_LICENSE)
                .setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100")
                // to reduce used native memory size
                .setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");

        config.getHotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(new File(folder, "hz_" + instanceIndex.incrementAndGet()));

        config.getNativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(128, MemoryUnit.MEGABYTES))
                .setMetadataSpacePercentage(20);
        return config;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE)
                .setSize(20);

        HotRestartConfig hrConfig = new HotRestartConfig().setEnabled(true);

        CacheConfig<K, V> cacheConfig = super.createCacheConfig();
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig)
                .setHotRestartConfig(hrConfig);
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
