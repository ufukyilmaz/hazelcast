package com.hazelcast.sql.index;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlOrderByTest;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;

import static com.hazelcast.sql.SqlBasicTest.serializationConfig;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDSqlOrderByTest extends SqlOrderByTest {

    private static final String MAP_NATIVE = "map_native";

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDIndexConfig();
    }

    @Override
    protected Config memberConfig() {
        return getConfig().setSerializationConfig(serializationConfig());
    }

    protected String mapName() {
        return MAP_NATIVE;
    }

    @Override
    @Test
    public void testSelectWithOrderByNoIndex() {
        try {
            checkSelectWithOrderBy(Collections.emptyList(),
                Arrays.asList("intVal"),
                Arrays.asList(true));
            fail("Order by without matching index should fail");
        } catch (HazelcastSqlException e) {
            assertContains(e.getMessage(), "Cannot query the IMap \"map_native\" with InMemoryFormat.NATIVE because it does not have global indexes");
        }

        try {
            checkSelectWithOrderBy(Collections.emptyList(),
                Arrays.asList("intVal", "realVal"),
                Arrays.asList(true, true));
            fail("Order by without matching index should fail");
        } catch (HazelcastSqlException e) {
            assertContains(e.getMessage(), "Cannot query the IMap \"map_native\" with InMemoryFormat.NATIVE because it does not have global indexes");
        }
    }
}
