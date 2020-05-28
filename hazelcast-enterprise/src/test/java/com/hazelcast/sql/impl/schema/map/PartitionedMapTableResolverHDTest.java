package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionedMapTableResolverHDTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private static final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory(1);
    private static HazelcastInstance instance;

    @Before
    public void beforeClass() {
        Config config = HDTestSupport.getHDConfig()
            .addMapConfig(new MapConfig(MAP_NAME).setInMemoryFormat(InMemoryFormat.NATIVE));

        instance = FACTORY.newHazelcastInstance(config);
    }

    @After
    public void afterClass() {
        FACTORY.shutdownAll();
    }

    @Test
    public void testNativeMap() {
        Map<Integer, Integer> map = instance.getMap(MAP_NAME);

        map.put(1, 1);

        Collection<Table> tables = new PartitionedMapTableResolver(nodeEngine(instance)).getTables();

        PartitionedMapTable table = (PartitionedMapTable) tables.iterator().next();

        checkFail(table::getFieldCount);
        checkFail(() -> table.getField(0));
        checkFail(table::getStatistics);
    }

    private void checkFail(Runnable code) {
        try {
            code.run();

            fail("Exception not thrown.");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.GENERIC, e.getCode());
            assertTrue(e.getMessage().contains("IMap with InMemoryFormat.NATIVE is not supported"));
        }
    }
}
