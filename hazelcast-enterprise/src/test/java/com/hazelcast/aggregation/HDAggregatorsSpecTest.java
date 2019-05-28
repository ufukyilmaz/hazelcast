package com.hazelcast.aggregation;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDAggregatorsSpecTest extends AggregatorsSpecTest {

    @Parameterized.Parameters(name = "{0} parallelAccumulation={1}, postfix={2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY, false, ""},
                {InMemoryFormat.OBJECT, false, ""},
                {InMemoryFormat.NATIVE, false, ""},
                {InMemoryFormat.BINARY, true, ""},
                {InMemoryFormat.OBJECT, true, ""},
                {InMemoryFormat.NATIVE, true, ""},

                {InMemoryFormat.BINARY, false, "[any]"},
                {InMemoryFormat.OBJECT, false, "[any]"},
                {InMemoryFormat.NATIVE, false, "[any]"},
                {InMemoryFormat.BINARY, true, "[any]"},
                {InMemoryFormat.OBJECT, true, "[any]"},
                {InMemoryFormat.NATIVE, true, "[any]"},
        });
    }

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }
}
