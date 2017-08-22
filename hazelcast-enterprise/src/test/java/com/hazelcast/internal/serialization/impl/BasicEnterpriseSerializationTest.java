package com.hazelcast.internal.serialization.impl;

import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;


@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class BasicEnterpriseSerializationTest extends AbstractSerializationServiceTest {

    private static Version V3_8 = Version.of("3.8");

    @Parameterized.Parameter
    public boolean versionedSerializationEnabled;

    public BasicEnterpriseSerializationTest() {
    }

    @Override
    protected AbstractSerializationService newAbstractSerializationService() {
        return (AbstractSerializationService) new EnterpriseSerializationServiceBuilder()
                .setClusterVersionAware(new BasicEnterpriseSerializationTest.TestVersionAware())
                .setVersionedSerializationEnabled(versionedSerializationEnabled)
                .setVersion(InternalSerializationService.VERSION_1)
                .build();
    }

    private class TestVersionAware implements EnterpriseClusterVersionAware {
        @Override
        public Version getClusterVersion() {
            return V3_8;
        }
    }

    @Parameterized.Parameters(name = "{index}: versionedSerializationEnabled = {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false}, {true}
        });
    }
}
