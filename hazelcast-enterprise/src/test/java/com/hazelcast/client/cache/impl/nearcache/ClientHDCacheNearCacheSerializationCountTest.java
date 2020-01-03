package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static com.hazelcast.internal.adapter.DataStructureAdapter.DataStructureMethods.GET;
import static com.hazelcast.internal.nearcache.HDNearCacheTestUtils.createNativeMemoryConfig;
import static java.util.Arrays.asList;

/**
 * HiDensity Near Cache serialization count tests for {@link com.hazelcast.cache.ICache} on Hazelcast clients.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientHDCacheNearCacheSerializationCountTest extends ClientCacheNearCacheSerializationCountTest {

    @Parameters(name = "method:{0} cacheFormat:{5} nearCacheFormat:{6} invalidateOnChange:{7} serializeKeys:{8} localUpdatePolicy:{9}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, null, null, null, null},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, NATIVE, true, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, NATIVE, true, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, NATIVE, true, false, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, NATIVE, true, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, NATIVE, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, NATIVE, false, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, NATIVE, false, false, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, NATIVE, false, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, BINARY, true, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, BINARY, true, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, BINARY, true, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, BINARY, true, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, BINARY, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, BINARY, false, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, BINARY, false, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, BINARY, false, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), NATIVE, OBJECT, true, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 0, 0), NATIVE, OBJECT, true, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), NATIVE, OBJECT, true, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 0, 0), NATIVE, OBJECT, true, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), NATIVE, OBJECT, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 0, 0), NATIVE, OBJECT, false, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), NATIVE, OBJECT, false, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 0, 0), NATIVE, OBJECT, false, false, CACHE_ON_UPDATE},

                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, null, null, null, null},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, NATIVE, true, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, NATIVE, true, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, NATIVE, true, false, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, NATIVE, true, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, NATIVE, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, NATIVE, false, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, NATIVE, false, false, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, NATIVE, false, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, false, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 0, 0), BINARY, OBJECT, false, true, CACHE_ON_UPDATE},

                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 1), newInt(1, 1, 1), OBJECT, null, null, null, null},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, NATIVE, true, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 1, 1), OBJECT, NATIVE, true, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, NATIVE, true, false, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 1, 1), OBJECT, NATIVE, true, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, NATIVE, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 1, 1), OBJECT, NATIVE, false, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, NATIVE, false, false, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 1, 1), OBJECT, NATIVE, false, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 1, 1), OBJECT, BINARY, false, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 0, 0), OBJECT, OBJECT, false, true, CACHE_ON_UPDATE},
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
