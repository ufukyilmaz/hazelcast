package com.hazelcast.cache.eviction;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseCacheExpirationTest extends CacheExpirationTest {

    @Rule
    public TestName testName = new TestName();

    @Parameters(name = "useSyncBackups:{0}, inMemoryFormat:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true, BINARY},
                {true, OBJECT},
                {true, NATIVE},
                {false, BINARY},
                {false, OBJECT},
                {false, NATIVE},
        });
    }

    @Parameter
    public boolean useSyncBackups;

    @Parameter(1)
    public InMemoryFormat inMemoryFormat;

    @Override
    protected Config getConfig() {
        Config config = getHDConfig(super.getConfig());
        if (testName.getMethodName().equals("test_correctBackupsAreExpired")) {
            config.setProperty(PROP_TASK_PERIOD_SECONDS, Integer.toString(MAX_VALUE));
        }
        return config;
    }

    @Override
    protected <K, V, M extends Serializable & ExpiryPolicy, T extends Serializable & CacheEntryListener<K, V>> CacheConfig<K, V> createCacheConfig(
            M expiryPolicy, T listener) {
        CacheConfig<K, V> cacheConfig = super.createCacheConfig(expiryPolicy, listener).setInMemoryFormat(inMemoryFormat);
        if (inMemoryFormat == NATIVE) {
            cacheConfig.setEvictionConfig(createEvictionConfig());
        }
        return cacheConfig;
    }

    private EvictionConfig createEvictionConfig() {
        return new EvictionConfig()
                .setSize(95)
                .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setEvictionPolicy(LRU);
    }

    @Override
    protected <K, V, M extends Serializable & ExpiryPolicy> CacheConfig<K, V> createCacheConfig(M expiryPolicy) {
        CacheConfig<K, V> cacheConfig = super.createCacheConfig(expiryPolicy);
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        if (inMemoryFormat == NATIVE) {
            cacheConfig.setEvictionConfig(createEvictionConfig());
        }
        return cacheConfig;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig<K, V> cacheConfig = super.createCacheConfig();
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        if (inMemoryFormat == NATIVE) {
            cacheConfig.setEvictionConfig(createEvictionConfig());
        }
        return cacheConfig;
    }
}
