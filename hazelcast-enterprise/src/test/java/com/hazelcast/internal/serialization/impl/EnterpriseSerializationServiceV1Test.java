package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseSerializationServiceV1Test {

    private static Version V3_8 = Version.of(3, 8, 0);

    @Test
    public void checkIfProperSerializerUsed_withRollingUpgrades() throws IOException {
        AbstractSerializationService ss = (AbstractSerializationService) new EnterpriseSerializationServiceBuilder()
                .setClusterVersionAware(new TestClusterVersionAware())
                .setRollingUpgradeEnabled(true)
                .build();

        assertEquals(EnterpriseDataSerializableSerializer.class, ss.dataSerializerAdapter.getImpl().getClass());
    }

    @Test
    public void checkIfProperSerializerUsed_withoutRollingUpgrades() throws IOException {
        AbstractSerializationService ss = (AbstractSerializationService) new EnterpriseSerializationServiceBuilder()
                .setRollingUpgradeEnabled(false)
                .build();

        assertEquals(DataSerializableSerializer.class, ss.dataSerializerAdapter.getImpl().getClass());
    }

    private static class TestClusterVersionAware implements EnterpriseClusterVersionAware {
        @Override
        public Version getClusterVersion() {
            return V3_8;
        }
    }

}