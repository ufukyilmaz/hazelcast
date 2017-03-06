package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.cache.nearcache.HiDensityNearCacheTestUtils.createNativeMemoryConfig;
import static com.hazelcast.cache.nearcache.HiDensityNearCacheTestUtils.getHDConfig;

public class ClientHDMapNearCachePreloaderTest extends ClientMapNearCachePreloaderTest {

    @Parameters(name = "invalidationOnChange:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false},
                {true},
        });
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testStoreAndLoad_withIntegerKeys_withInMemoryFormatNative() {
        storeAndLoad(2342, KeyType.INTEGER, InMemoryFormat.NATIVE);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testStoreAndLoad_withStringKeys_withInMemoryFormatNative() {
        storeAndLoad(4223, KeyType.STRING, InMemoryFormat.NATIVE);
    }

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }

    @Override
    protected ClientConfig getClientConfig() {
        return super.getClientConfig()
                .setLicenseKey(SampleLicense.UNLIMITED_LICENSE)
                .setNativeMemoryConfig(createNativeMemoryConfig());
    }
}
