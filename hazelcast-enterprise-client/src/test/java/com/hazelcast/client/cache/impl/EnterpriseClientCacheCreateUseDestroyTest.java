package com.hazelcast.client.cache.impl;

import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
public class EnterpriseClientCacheCreateUseDestroyTest extends ClientCacheCreateUseDestroyTest {

    @Override
    protected void assumptions() {
    }
}
