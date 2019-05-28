package com.hazelcast.internal.serialization.impl;

import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BasicEnterpriseSerializationTest extends AbstractSerializationServiceTest {

    private boolean versionedSerializationEnabled;

    public BasicEnterpriseSerializationTest(boolean versionedSerializationEnabled) {
        this.versionedSerializationEnabled = versionedSerializationEnabled;
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
            return Versions.V3_10;
        }
    }

    @Parameterized.Parameters(name = "{index}: versionedSerializationEnabled = {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false},
                {true},
        });
    }
}
