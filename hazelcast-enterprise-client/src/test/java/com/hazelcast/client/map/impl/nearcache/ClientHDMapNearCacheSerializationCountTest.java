package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
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

import static com.hazelcast.cache.nearcache.HiDensityNearCacheTestUtils.createNativeMemoryConfig;
import static com.hazelcast.cache.nearcache.HiDensityNearCacheTestUtils.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static java.util.Arrays.asList;

/**
 * HiDensity Near Cache serialization count tests for {@link com.hazelcast.core.IMap} on Hazelcast clients.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientHDMapNearCacheSerializationCountTest extends ClientMapNearCacheSerializationCountTest {

    @Parameters(name = "mapFormat:{4} nearCacheFormat:{5}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, null},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, NATIVE},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), NATIVE, BINARY},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), NATIVE, OBJECT},

                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, null},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, NATIVE},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT},

                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 1), newInt(1, 1, 1), OBJECT, null},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, NATIVE},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT},
        });
    }

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }

    @Override
    protected ClientConfig getClientConfig() {
        return new ClientConfig()
                .setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), UNLIMITED_LICENSE)
                .setNativeMemoryConfig(createNativeMemoryConfig());
    }
}
