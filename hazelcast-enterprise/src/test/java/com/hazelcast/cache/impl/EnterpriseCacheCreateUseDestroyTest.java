package com.hazelcast.cache.impl;

import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(QuickTest.class)
public class EnterpriseCacheCreateUseDestroyTest extends CacheCreateUseDestroyTest {

    @Override
    protected void assumptions() {
    }
}
