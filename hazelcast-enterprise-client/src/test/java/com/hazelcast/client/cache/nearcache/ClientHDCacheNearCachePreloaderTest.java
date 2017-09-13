package com.hazelcast.client.cache.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.internal.nearcache.HiDensityNearCacheTestUtils.createNativeMemoryConfig;
import static com.hazelcast.internal.nearcache.HiDensityNearCacheTestUtils.getNearCacheHDConfig;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientHDCacheNearCachePreloaderTest extends ClientCacheNearCachePreloaderTest {

    @Parameters(name = "format:{0} invalidationOnChange:{1} serializeKeys:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, false, true},
                {InMemoryFormat.NATIVE, false, false},
                {InMemoryFormat.NATIVE, true, true},
                {InMemoryFormat.NATIVE, true, false},

                {InMemoryFormat.BINARY, false, true},
                {InMemoryFormat.BINARY, false, false},
                {InMemoryFormat.BINARY, true, true},
                {InMemoryFormat.BINARY, true, false},

                {InMemoryFormat.OBJECT, false, true},
                {InMemoryFormat.OBJECT, false, false},
                {InMemoryFormat.OBJECT, true, true},
                {InMemoryFormat.OBJECT, true, false},
        });
    }

    @Override
    protected Config getConfig() {
        return getNearCacheHDConfig();
    }

    @Override
    protected ClientConfig getClientConfig() {
        return super.getClientConfig()
                .setLicenseKey(SampleLicense.UNLIMITED_LICENSE)
                .setNativeMemoryConfig(createNativeMemoryConfig());
    }

    @Test
    @Override
    public void testCreateAndDestroyDataStructure_withSameName() {
        if (inMemoryFormat == InMemoryFormat.NATIVE) {
            // FIXME: https://github.com/hazelcast/hazelcast-enterprise/issues/1660
            return;
        }
        super.testCreateAndDestroyDataStructure_withSameName();
    }

    @Test
    @Override
    public void testCreateAndDestroyDataStructure_withDifferentNames() {
        if (inMemoryFormat == InMemoryFormat.NATIVE) {
            // FIXME: https://github.com/hazelcast/hazelcast-enterprise/issues/1660
            return;
        }
        super.testCreateAndDestroyDataStructure_withDifferentNames();
    }
}
