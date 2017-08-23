package com.hazelcast.cache.impl;

import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
public class EnterpriseCacheCreateUseDestroyTest extends CacheCreateUseDestroyTest {

    @Override
    protected void assumptions() {
    }
}
