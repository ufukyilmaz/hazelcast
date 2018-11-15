package com.hazelcast.internal.management;

import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Verifies scripting protection feature on Hazelcast Enterprise. It differs from OS in a default value used for the feature.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ QuickTest.class, ParallelTest.class })
public class EnterpriseScriptingProtectionTest extends ScriptingProtectionTest {

    protected boolean getScriptingEnabledDefaultValue() {
        // Hazelcast Enterprise has the scripting disabled by default.
        return false;
    }

}
