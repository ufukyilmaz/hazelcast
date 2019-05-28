package com.hazelcast.enterprise;

import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.junit.runner.Runner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.ParametersRunnerFactory;

import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;

/**
 * {@link ParametersRunnerFactory} implementation which creates {@link HazelcastParallelClassRunner}
 * to run test methods in parallel.
 * <p>
 * See {@link com.hazelcast.test package documentation} for runners overview.
 */
public class EnterpriseParallelParametersRunnerFactory extends HazelcastParametersRunnerFactory {

    @Override
    protected Runner getClassRunner(Class<?> testClass, Object[] parameters, String testName)
            throws InitializationError {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(UNLIMITED_LICENSE);
        return new EnterpriseParallelJUnitClassRunner(testClass, parameters, testName);
    }
}
