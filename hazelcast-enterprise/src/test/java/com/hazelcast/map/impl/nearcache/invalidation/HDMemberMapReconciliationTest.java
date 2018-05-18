package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMemberMapReconciliationTest extends MemberMapReconciliationTest {

    @Parameters(name = "mapInMemoryFormat:{0} nearCacheInMemoryFormat:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, BINARY},
                {BINARY, OBJECT},
                {BINARY, NATIVE},

                {OBJECT, BINARY},
                {OBJECT, OBJECT},
                {OBJECT, NATIVE},

                {NATIVE, BINARY},
                {NATIVE, OBJECT},
                {NATIVE, NATIVE},
        });
    }

    @Override
    protected Config getConfig() {
        return getHDConfig(super.getConfig());
    }
}
