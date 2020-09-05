package com.hazelcast.sql.index;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test that ensures that we properly handle misconfigured cluster, when one member has global HD indexes, while another has
 * local HD indexes.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDNonConcurrentIndexOnOneMemberTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
    private HazelcastInstance goodMember;

    @Before
    public void before() {
        goodMember = factory.newHazelcastInstance(config(true));
        HazelcastInstance badMember = factory.newHazelcastInstance(config(false));

        goodMember.getMap(MAP_NAME).put(getLocalKey(goodMember, value -> value), 1);
        badMember.getMap(MAP_NAME).put(getLocalKey(badMember, value -> value), 1);
    }

    private Config config(boolean global) {
        MapConfig mapConfig = new MapConfig().setName(MAP_NAME).setInMemoryFormat(InMemoryFormat.NATIVE);
        mapConfig.addIndexConfig(new IndexConfig().setName("index").addAttribute("this"));

        Config config = HDTestSupport.getSmallInstanceHDIndexConfig().addMapConfig(mapConfig);

        config.setProperty(ClusterProperty.GLOBAL_HD_INDEX_ENABLED.getName(), Boolean.toString(global));

        return config;
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void test_index_misconfiguration() {
        try {
            execute(goodMember, "SELECT * FROM " + MAP_NAME);

            fail("Must fail");
        } catch (HazelcastSqlException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            assertEquals("Cannot use the index \"index\" of the IMap \"map\" because it is not global (make sure the property \"hazelcast.hd.global.index.enabled\" is set to \"true\")", e.getMessage());
        }
    }
}
