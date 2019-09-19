package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.security.AccessControlException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutorSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String testObjectName = randomString();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testExecutorPermission() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, testObjectName, "dev")
                .addAction(ActionConstants.ACTION_CREATE);

        addNonExecutorPermissions(config);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        assertEquals(new Integer(11), client.getExecutorService(testObjectName).submit(new DummyCallable()).get());
    }

    @Test
    public void testExecutorPermissionFail() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, testObjectName, "dev")
                .addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        Future<Integer> future = client.getExecutorService(testObjectName).submit(new DummyCallable());
        expectedException.expect(ExecutionException.class);
        future.get();
    }

    @Test
    public void testExecutorPermissionFail2() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, testObjectName, "dev");
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        expectedException.expect(AccessControlException.class);
        client.getExecutorService(testObjectName);
    }

    @Test
    public void testExecutorPermissionFail3() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, testObjectName, "dev")
                .addAction(ActionConstants.ACTION_CREATE);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        assertNull(client.getExecutorService(testObjectName).submit(new DummyCallableNewThread()).get());
    }

    static class DummyCallable implements Callable<Integer>, Serializable, HazelcastInstanceAware {
        transient HazelcastInstance hz;

        @Override
        public Integer call() throws Exception {
            int result = 0;

            hz.getMap("map").put("key", "value");
            result += hz.getMap("map").size(); // +1

            hz.getQueue("queue").add("value");
            result += hz.getQueue("queue").size(); // +1

            hz.getTopic("topic").publish("value");
            result++; // +1

            hz.getMultiMap("multimap").put("key", "value");
            result += hz.getMultiMap("multimap").size(); // +1

            hz.getList("list").add("value");
            result += hz.getList("list").size(); // +1

            hz.getSet("set").add("value");
            result += hz.getSet("set").size(); // +1

            hz.getFlakeIdGenerator("flake_id_generator").newId();
            result++; // +1

            hz.getLock("lock").lock();
            hz.getLock("lock").unlock();
            result++; // +1

            result += hz.getAtomicLong("atomic_long").incrementAndGet(); // +1

            hz.getCPSubsystem().getCountDownLatch("countdown_latch").trySetCount(2);
            hz.getCPSubsystem().getCountDownLatch("countdown_latch").countDown();
            result += hz.getCPSubsystem().getCountDownLatch("countdown_latch").getCount(); // +1

            hz.getSemaphore("semaphore").init(2);
            hz.getSemaphore("semaphore").acquire();
            result += hz.getSemaphore("semaphore").availablePermits(); // +1

            return result;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            hz = hazelcastInstance;
        }
    }

    static class DummyCallableNewThread implements Callable<Integer>, Serializable, HazelcastInstanceAware {
        transient HazelcastInstance hz;

        @Override
        public Integer call() throws Exception {
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<Integer> value = new AtomicReference<Integer>();
            new Thread() {
                @Override
                public void run() {
                    try {
                        hz.getList("list").add("value");
                        value.set(hz.getList("list").size());
                    } finally {
                        latch.countDown();
                    }
                }
            }.start();
            latch.await();
            return value.get();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            hz = hazelcastInstance;
        }
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config, PermissionType type, String name, String principal) {
        PermissionConfig perm = new PermissionConfig(type, name, principal);
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }

    private void addAllPermission(Config config, PermissionType type, String name) {
        addPermission(config, type, name, null).addAction(ActionConstants.ACTION_ALL);
    }

    private void addNonExecutorPermissions(Config config) {
        addAllPermission(config, PermissionType.MAP, "map");
        addAllPermission(config, PermissionType.QUEUE, "queue");
        addAllPermission(config, PermissionType.TOPIC, "topic");
        addAllPermission(config, PermissionType.MULTIMAP, "multimap");
        addAllPermission(config, PermissionType.LIST, "list");
        addAllPermission(config, PermissionType.SET, "set");
        addAllPermission(config, PermissionType.ID_GENERATOR, "id_generator");
        addAllPermission(config, PermissionType.LOCK, "lock");
        addAllPermission(config, PermissionType.ATOMIC_LONG, "atomic_long");
        addAllPermission(config, PermissionType.COUNTDOWN_LATCH, "countdown_latch");
        addAllPermission(config, PermissionType.SEMAPHORE, "semaphore");
        addAllPermission(config, PermissionType.FLAKE_ID_GENERATOR, "flake_id_generator");
    }
}
