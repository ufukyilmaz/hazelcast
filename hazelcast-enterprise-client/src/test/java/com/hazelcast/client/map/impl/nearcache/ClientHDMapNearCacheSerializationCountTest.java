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

    @Parameters(name = "mapFormat:{2} nearCacheFormat:{3}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, NATIVE, null,},
                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, NATIVE, NATIVE,},
                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, NATIVE, BINARY,},
                {new int[]{1, 0, 0}, new int[]{0, 1, 0}, NATIVE, OBJECT,},

                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, BINARY, null,},
                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, BINARY, NATIVE,},
                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, BINARY, BINARY,},
                {new int[]{1, 0, 0}, new int[]{0, 1, 0}, BINARY, OBJECT,},

                {new int[]{1, 1, 1}, new int[]{1, 1, 1}, OBJECT, null,},
                {new int[]{1, 1, 0}, new int[]{1, 1, 1}, OBJECT, NATIVE,},
                {new int[]{1, 1, 0}, new int[]{1, 1, 1}, OBJECT, BINARY,},
                {new int[]{1, 1, 0}, new int[]{1, 1, 0}, OBJECT, OBJECT,},
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
