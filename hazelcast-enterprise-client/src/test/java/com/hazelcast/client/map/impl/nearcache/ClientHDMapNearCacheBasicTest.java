package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.internal.nearcache.HiDensityNearCacheTestUtils.createNativeMemoryConfig;
import static com.hazelcast.internal.nearcache.HiDensityNearCacheTestUtils.getNearCacheHDConfig;
import static java.util.Arrays.asList;

/**
 * Basic HiDensity Near Cache tests for {@link com.hazelcast.core.IMap} on Hazelcast clients.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientHDMapNearCacheBasicTest extends ClientMapNearCacheBasicTest {

    @Parameters(name = "format:{0} serializeKeys:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, true},
                {InMemoryFormat.NATIVE, false},

                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, false},
        });
    }

    @Override
    protected Config getConfig() {
        return getNearCacheHDConfig();
    }

    @Override
    protected ClientConfig createClientConfig() {
        return super.createClientConfig()
                .setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), UNLIMITED_LICENSE)
                .setNativeMemoryConfig(createNativeMemoryConfig());
    }
}
