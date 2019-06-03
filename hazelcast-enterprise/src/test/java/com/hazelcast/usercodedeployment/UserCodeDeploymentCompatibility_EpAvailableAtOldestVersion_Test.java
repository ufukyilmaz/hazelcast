package com.hazelcast.usercodedeployment;

import com.hazelcast.config.Config;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.internal.usercodedeployment.impl.filter.UserCodeDeploymentAbstractTest;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getKnownPreviousVersionsCount;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class UserCodeDeploymentCompatibility_EpAvailableAtOldestVersion_Test extends UserCodeDeploymentAbstractTest {

    @Parameters(name = "ClassCacheMode:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {UserCodeDeploymentConfig.ClassCacheMode.ETERNAL},
                {UserCodeDeploymentConfig.ClassCacheMode.OFF},
        });
    }

    @Parameter
    public volatile UserCodeDeploymentConfig.ClassCacheMode classCacheMode;

    @Override
    protected UserCodeDeploymentConfig.ClassCacheMode getClassCacheMode() {
        return classCacheMode;
    }

    @Override
    protected TestHazelcastInstanceFactory newFactory() {
        return new CompatibilityTestHazelcastInstanceFactory();
    }

    @Test
    @Override
    public void givenSomeMemberCanAccessTheEP_whenTheEPIsFilteredLocally_thenItWillBeLoadedOverNetwork_anonymousInnerClasses() {
        super.givenSomeMemberCanAccessTheEP_whenTheEPIsFilteredLocally_thenItWillBeLoadedOverNetwork_anonymousInnerClasses();
    }

    @Override
    protected void executeSimpleTestScenario(Config config,
                                             Config epFilteredConfig,
                                             EntryProcessor<Integer, Integer, Integer> ep) {
        int keyCount = 100;

        lowerOperationTimeouts(config);
        lowerOperationTimeouts(epFilteredConfig);

        factory = newFactory();
        HazelcastInstance instanceWithNewEp = factory.newHazelcastInstance(config);
        factory.newInstances(epFilteredConfig, getKnownPreviousVersionsCount());

        IMap<Integer, Integer> map = instanceWithNewEp.getMap(randomName());

        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0);
        }
        map.executeOnEntries(ep);
        for (int i = 0; i < keyCount; i++) {
            assertEquals(1, (int) map.get(i));
        }
    }
}
