package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.SampleLicense;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.cache.nearcache.HiDensityNearCacheTestUtils.createNativeMemoryConfig;
import static com.hazelcast.cache.nearcache.HiDensityNearCacheTestUtils.getHDConfig;

public class ClientHDMapNearCachePreloaderTest extends ClientMapNearCachePreloaderTest {

    @Parameters(name = "format:{0} invalidationOnChange:{1}")
    public static Collection<Object[]> parameters() {
        // FIXME: the Near Cache pre-loader doesn't work with enabled invalidations due to a known getAll() issue!
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY, false},
                //{InMemoryFormat.BINARY, true},
                {InMemoryFormat.OBJECT, false},
                //{InMemoryFormat.OBJECT, true},
                {InMemoryFormat.NATIVE, false},
                //{InMemoryFormat.NATIVE, true},
        });
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
