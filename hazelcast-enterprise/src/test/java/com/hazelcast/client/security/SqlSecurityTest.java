package com.hazelcast.client.security;

import com.hazelcast.HDTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.map.IMap;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for SQL security permissions.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlSecurityTest {

    private static final String MAP_NAME = "map";
    private static final String EXECUTOR_NAME = "exec";

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @Parameterized.Parameter
    public SecurityMode mode;

    @Parameterized.Parameter(1)
    public boolean hd;

    private HazelcastInstance client;

    @Parameterized.Parameters(name = "mode:{0}, hd:{1}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (SecurityMode mode : SecurityMode.values()) {
            for (boolean hd : Arrays.asList(false, true)) {
                res.add(new Object[] { mode, hd });
            }
        }

        return res;
    }

    @Before
    public void before() {
        HazelcastInstance member = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Integer> map = member.getMap(MAP_NAME);
        map.addIndex(IndexType.SORTED, "this");
        map.put(1, 1);

        client = factory.newHazelcastClient();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    private Config getConfig() {
        Config config;

        if (hd) {
            config = HDTestSupport.getSmallInstanceHDIndexConfig();
        } else {
            config = HazelcastTestSupport.smallInstanceConfig();
        }

        if (mode != SecurityMode.DISABLED) {
            SecurityConfig securityCOnfig = config.getSecurityConfig();

            securityCOnfig.setEnabled(true);

            securityCOnfig.addClientPermissionConfig(
                new PermissionConfig(PermissionType.EXECUTOR_SERVICE, EXECUTOR_NAME, null).addAction(ActionConstants.ACTION_CREATE)
            );

            if (mode == SecurityMode.ENABLED_ALLOWED) {
                securityCOnfig.addClientPermissionConfig(
                    new PermissionConfig(PermissionType.MAP, MAP_NAME, null).addAction(ActionConstants.ACTION_READ)
                );
            }
        }

        return config;
    }

    @Test
    public void testSecurity() {
        // Check map scan
        checkSql("SELECT * FROM " + MAP_NAME);

        // Check index scan
        checkSql("SELECT * FROM " + MAP_NAME + " WHERE this = 1");
    }

    private void checkSql(String sql) {
        // Run directly
        check(sql(sql, false));
        check(sql(sql, true));

        // Run through executor
        check(executorSql(sql, false));
        check(executorSql(sql, true));
    }

    private void check(Runnable runnable) {
        if (mode.shouldPass) {
            runnable.run();
        } else {
            try {
                runnable.run();

                fail("Must fail");
            } catch (Exception e) {
                assertEquals(AccessControlException.class, e.getClass());
                assertThat(e.getMessage(), allOf(containsString("MapPermission"), containsString("map"), containsString("read")));
            }
        }
    }

    private Runnable sql(String sql, boolean statement) {
        if (statement) {
            return () -> client.getSql().execute(new SqlStatement(sql));
        } else {
            return () -> client.getSql().execute(sql);
        }
    }

    private Runnable executorSql(String sql, boolean statement) {
        return () -> {
            try {
                client.getExecutorService(EXECUTOR_NAME).submit(new SqlRunnable(sql, statement)).get();
            } catch (Exception e) {
                if (e instanceof ExecutionException) {
                    Throwable cause = e.getCause();

                    if (cause instanceof AccessControlException) {
                        throw (RuntimeException) cause;
                    }
                }

                throw new RuntimeException(e);
            }
        };
    }

    private static class SqlRunnable implements Runnable, Serializable, HazelcastInstanceAware {

        private final String sql;
        private final boolean useStatement;

        private transient HazelcastInstance instance;

        private SqlRunnable(String sql, boolean useStatement) {
            this.sql = sql;
            this.useStatement = useStatement;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void run() {
            if (useStatement) {
                instance.getSql().execute(new SqlStatement(sql)).close();
            } else {
                instance.getSql().execute(sql);
            }
        }
    }

    private enum SecurityMode {

        DISABLED(true),
        ENABLED_ALLOWED(true);
        // Ignore, investigate in https://github.com/hazelcast/hazelcast-enterprise/issues/3910
        //ENABLED_DENIED(false);

        private final boolean shouldPass;

        SecurityMode(boolean shouldPass) {
            this.shouldPass = shouldPass;
        }
    }
}
