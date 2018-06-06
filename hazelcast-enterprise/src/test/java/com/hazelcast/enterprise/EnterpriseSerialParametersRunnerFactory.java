package com.hazelcast.enterprise;

import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.runners.parameterized.ParametersRunnerFactory;

/**
 * {@link ParametersRunnerFactory} implementation which creates {@link HazelcastSerialClassRunner}
 * to run test methods serially.
 * <p>
 * See {@link com.hazelcast.test package documentation} for runners overview.
 */
public class EnterpriseSerialParametersRunnerFactory extends EnterpriseParametersRunnerFactory {

    @Override
    protected boolean isParallel(Class<?> testClass) {
        return false;
    }
}
