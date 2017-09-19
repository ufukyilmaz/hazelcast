package com.hazelcast.client.cache.impl;

import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(QuickTest.class)
public class EnterpriseClientCacheCreateUseDestroyTest extends ClientCacheCreateUseDestroyTest {

    @Override
    protected void assumptions() {
    }
}
