package com.hazelcast.sql.index;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
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
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.HDTestSupport.getSmallInstanceHDIndexConfig;
import static com.hazelcast.spi.properties.ClusterProperty.GLOBAL_HD_INDEX_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test that ensures that non-global indexes are not used during query execution.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDNonConcurrentSqlIndexTest extends SqlTestSupport {

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
    private HazelcastInstance member;

    @Parameterized.Parameter
    public boolean hasIndex;

    @Parameterized.Parameters(name = "hasIndex:{0}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        res.add(new Object[] { false });
        res.add(new Object[] { true });

        return res;
    }

    @Before
    public void before() {
        member = factory.newHazelcastInstance(getConfig());

        member.getMap("map").put(1, 1);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testFailure() {
        try {
            execute(member, "SELECT this FROM map");

            fail("Must fail");
        } catch (HazelcastSqlException e) {
            assertEquals(SqlErrorCode.GENERIC, e.getCode());
            assertEquals(
                "Cannot query the IMap \"map\" with InMemoryFormat.NATIVE because it does not have global indexes (please make sure that the IMap has at least one index, and the property \"hazelcast.hd.global.index.enabled\" is set to \"true\")",
                e.getMessage()
            );
        }
    }

    @Override
    protected Config getConfig() {
        Config config = getSmallInstanceHDIndexConfig();
        config.setProperty(GLOBAL_HD_INDEX_ENABLED.getName(), "false");

        MapConfig mapConfig = new MapConfig().setName("map").setInMemoryFormat(InMemoryFormat.NATIVE);

        if (hasIndex) {
            mapConfig.addIndexConfig(new IndexConfig().setName("index").addAttribute("this"));
        }

        config.addMapConfig(mapConfig);

        return config;
    }
}
