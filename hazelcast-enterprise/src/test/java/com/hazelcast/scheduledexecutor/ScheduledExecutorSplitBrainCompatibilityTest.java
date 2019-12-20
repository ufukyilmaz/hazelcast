package com.hazelcast.scheduledexecutor;

import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorSplitBrainTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class ScheduledExecutorSplitBrainCompatibilityTest extends ScheduledExecutorSplitBrainTest {
}
