package com.hazelcast.client.cache.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.internal.nearcache.HDNearCacheTestUtils.createNativeMemoryConfig;
import static java.util.Arrays.asList;

/**
 * Basic HiDensity Near Cache tests for {@link com.hazelcast.cache.ICache} on Hazelcast clients.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ClientHDCacheNearCacheBasicSlowTest extends ClientCacheNearCacheBasicSlowTest {

    @Parameters(name = "format:{0} serializeKeys:{1} localUpdatePolicy:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, true, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.NATIVE, true, LocalUpdatePolicy.CACHE_ON_UPDATE},
                {InMemoryFormat.NATIVE, false, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.NATIVE, false, LocalUpdatePolicy.CACHE_ON_UPDATE},

                {InMemoryFormat.BINARY, false, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.BINARY, false, LocalUpdatePolicy.CACHE_ON_UPDATE},
                {InMemoryFormat.OBJECT, false, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.OBJECT, false, LocalUpdatePolicy.CACHE_ON_UPDATE},
        });
    }

    @Override
    protected Config getConfig() {
        return getHDConfig(super.getConfig());
    }

    @Override
    protected ClientConfig getClientConfig() {
        return super.getClientConfig()
                .setNativeMemoryConfig(createNativeMemoryConfig());
    }
}
