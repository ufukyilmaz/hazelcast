package com.hazelcast.map;

import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class HDMapSetTtlBackupCompatibilityTest extends HDMapSetTtlBackupTest {
}
