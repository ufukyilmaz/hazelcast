package com.hazelcast.map;

import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.setFirstInstanceToNonProxyInstance;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class HDMapSetTTLBackupCompatibilityTest extends HDMapSetTTLBackupTest {

    @Rule
    // RU_COMPAT_3_10
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    @Override
    // RU_COMPAT_3_10
    public void setup() {
        super.setup();
        setFirstInstanceToNonProxyInstance(instances);
    }

    @Test
    @Override
    // RU_COMPAT_3_10
    public void testBackups() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Modifying TTL is available when cluster version is 3.11 or higher");
        super.testBackups();
    }

    @Test
    @Override
    // RU_COMPAT_3_10
    public void testMakesTempBackupEntriesUnlimited() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Modifying TTL is available when cluster version is 3.11 or higher");
        super.testMakesTempBackupEntriesUnlimited();
    }
}
