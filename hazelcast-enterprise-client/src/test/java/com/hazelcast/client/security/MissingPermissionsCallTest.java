package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.config.PermissionConfig.PermissionType.ATOMIC_LONG;
import static com.hazelcast.config.PermissionConfig.PermissionType.DURABLE_EXECUTOR_SERVICE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class})
public class MissingPermissionsCallTest
        extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private static final String ATOMIC_LONG_REF = "foobar";
    private static final String DURABLE_EXEC_NAME = "ExecKungfu";
    private static final String PERMISSION_PRINCIPAL = "dev";
    private static final String PERMISSION_NAME = "*";

    @Parameterized.Parameters(name = "policy:{0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(true);
    }

    @Parameterized.Parameter
    public boolean policy;

    private HazelcastInstance client;

    private Long response;

    private Throwable error;

    @Before
    public void setup() {
        Config config = new Config();

        SecurityConfig sc = new SecurityConfig();
        sc.setEnabled(true);
        sc.setClientBlockUnmappedActions(policy);
        PermissionConfig execPermission = new PermissionConfig(DURABLE_EXECUTOR_SERVICE, PERMISSION_NAME, PERMISSION_PRINCIPAL);
        execPermission.addAction("create");
        sc.addClientPermissionConfig(execPermission);

        PermissionConfig atomicLongPermission = new PermissionConfig(ATOMIC_LONG, PERMISSION_NAME, PERMISSION_PRINCIPAL);
        atomicLongPermission.addAction("create");
        atomicLongPermission.addAction("modify");
        atomicLongPermission.addAction("read");
        sc.addClientPermissionConfig(atomicLongPermission);
        config.setSecurityConfig(sc);

        factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void verifyCorrectAccessRights_whenRunWithinExecutor_whenDenyAccessIsDefined() {
        final CountDownLatch lock = new CountDownLatch(1);

        DurableExecutorService executor = client.getDurableExecutorService(DURABLE_EXEC_NAME);
        executor.submit(new SampleTask()).andThen(new ExecutionCallback<Long>() {
            @Override
            public void onResponse (Long response){
                MissingPermissionsCallTest.this.response = response;
                lock.countDown();
            }

            @Override
            public void onFailure (Throwable t){
                MissingPermissionsCallTest.this.error = t;
                lock.countDown();
            }
        });

        try {
            lock.await(10, SECONDS);
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }

        if (policy) {
            assertEquals(AccessControlException.class, error.getCause().getClass());
            assertThat(error.getMessage(), allOf(containsString("permission-mapping"), containsString("addAndGetAsync")));
        } else {
            assertEquals(2L, (long) response);
        }
    }

    static class SampleTask implements Callable<Long>, Serializable, HazelcastInstanceAware {

        private transient HazelcastInstance instance;
        private long value = 2L;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }

        @Override
        public Long call() {
            IAtomicLong atomicLong = instance.getAtomicLong(ATOMIC_LONG_REF);
            atomicLong.addAndGetAsync(value);
            return atomicLong.get();
        }
    }

}