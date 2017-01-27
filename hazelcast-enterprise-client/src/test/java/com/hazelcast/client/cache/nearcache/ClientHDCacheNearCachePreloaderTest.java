package com.hazelcast.client.cache.nearcache;

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

public class ClientHDCacheNearCachePreloaderTest extends ClientCacheNearCachePreloaderTest {

    @Parameters(name = "invalidationOnChange:{0}")
    public static Collection<Object[]> parameters() {
        // FIXME: the Near Cache pre-loader doesn't work with enabled invalidations due to a known getAll() issue!
        return Arrays.asList(new Object[][]{
                {false},
                //{true},
        });
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testStoreAndLoad_withIntegerKeys_withInMemoryFormatNative() {
        storeAndLoad(2342, false, InMemoryFormat.NATIVE);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testStoreAndLoad_withStringKeys_withInMemoryFormatNative() {
        storeAndLoad(4223, true, InMemoryFormat.NATIVE);
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
