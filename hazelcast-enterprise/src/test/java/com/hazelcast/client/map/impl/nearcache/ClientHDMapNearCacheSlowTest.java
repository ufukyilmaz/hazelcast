package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ClientHDMapNearCacheSlowTest extends ClientMapNearCacheSlowTest {

    @Override
    protected Config newConfig() {
        Config config = getHDConfig(super.newConfig())
                .setLicenseKey(UNLIMITED_LICENSE);
        config.getMapConfig("default")
                .setInMemoryFormat(NATIVE);
        return config;
    }

    @Override
    protected ClientConfig newClientConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(32, MemoryUnit.MEGABYTES))
                .setAllocatorType(MemoryAllocatorType.STANDARD);

        return super.newClientConfig()
                .setNativeMemoryConfig(nativeMemoryConfig);
    }

    @Override
    protected NearCacheConfig newNearCacheConfig() {
        return super.newNearCacheConfig()
                .setInMemoryFormat(NATIVE);
    }

    @Override
    protected NearCacheConfig newNearCacheConfigWithEntryCountEviction(EvictionPolicy evictionPolicy, int size) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(evictionPolicy)
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(size);

        return newNearCacheConfig()
                .setEvictionConfig(evictionConfig);
    }

    @Test
    @Override
    public void testNearCache_whenInMemoryFormatIsNative_thenThrowIllegalArgumentException() {
        // this test expects an IllegalArgumentException in OS, but should not throw any exception in EE
        super.testNearCache_whenInMemoryFormatIsNative_thenThrowIllegalArgumentException();
    }
}
