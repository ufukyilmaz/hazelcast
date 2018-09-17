package com.hazelcast.enterprise;

public class EnterpriseParallelParametersRunnerFactory extends EnterpriseParametersRunnerFactory {

    @Override
    protected boolean isParallel(Class<?> testClass) {
        return true;
    }
}