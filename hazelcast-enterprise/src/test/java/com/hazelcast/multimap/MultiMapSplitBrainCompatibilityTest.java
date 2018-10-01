package com.hazelcast.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class MultiMapSplitBrainCompatibilityTest extends MultiMapSplitBrainTest {

    public static final String V3_10_5 = "3.10.5";

    // Test with 3.10.5 + 3.11 members because
    // https://github.com/hazelcast/hazelcast/issues/13559 obstructs MultiMap
    // split-brain healing and is fixed since 3.10.5
    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        HazelcastInstance[] hazelcastInstances = new HazelcastInstance[clusterSize];
        factory = new CompatibilityTestHazelcastInstanceFactory(new String[] {V3_10_5, V3_10_5, CURRENT_VERSION});
        for (int i = 0; i < clusterSize; i++) {
            HazelcastInstance hz = factory.newHazelcastInstance(config);
            hazelcastInstances[i] = hz;
        }
        return hazelcastInstances;
    }

    @Override
    protected void onTearDown() {
        factory.terminateAll();
    }
}
