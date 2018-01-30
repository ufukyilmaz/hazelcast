package com.hazelcast.client.cache.nearcache.invalidation;

import com.hazelcast.client.cache.impl.nearcache.invalidation.CacheMetaDataGeneratorTest;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.enterprise.SampleLicense.ENTERPRISE_HD_LICENSE;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDCacheMetaDataGeneratorTest extends CacheMetaDataGeneratorTest {

    private static final MemorySize CLIENT_NATIVE_MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Override
    protected NearCacheConfig newNearCacheConfig() {
        NearCacheConfig nearCacheConfig = super.newNearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        return nearCacheConfig;
    }

    @Override
    protected ClientConfig newClientConfig() {
        ClientConfig clientConfig = super.newClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), ENTERPRISE_HD_LICENSE);
        clientConfig.setNativeMemoryConfig(new NativeMemoryConfig().setSize(CLIENT_NATIVE_MEMORY_SIZE).setEnabled(true));

        return clientConfig;
    }
}
