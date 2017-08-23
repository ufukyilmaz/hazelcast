package com.hazelcast.client.cache.impl;

import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import org.junit.Ignore;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Ignore(value = "https://github.com/hazelcast/hazelcast-enterprise/issues/1653")
public class EnterpriseClientCacheCreateUseDestroyTest extends ClientCacheCreateUseDestroyTest {

    @Override
    protected void assumptions() {
    }
}
