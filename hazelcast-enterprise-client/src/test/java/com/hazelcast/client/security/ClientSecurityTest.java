package com.hazelcast.client.security;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.security.AccessControlException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientSecurityTest {

    TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test(expected = RuntimeException.class)
    public void testDenyAll() {
        final Config config = createConfig();
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            client.getMap("test").size();
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testAllowAll() {
        final Config config = createConfig();
        addPermission(config, PermissionType.ALL, "", null);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            client.getMap("test").size();
            client.getMap("test").size();
            client.getMap("test").put("a", "b");
            client.getQueue("Q").poll();
        } finally {
            client.shutdown();
        }
    }

    @Test(expected = RuntimeException.class)
    public void testDenyEndpoint() {
        final Config config = createConfig();
        final PermissionConfig pc = addPermission(config, PermissionType.ALL, "", "dev");
        pc.addEndpoint("10.10.10.*");

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            client.getMap("test").size();
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testMapAllPermission() {
        final Config config = createConfig();
        PermissionConfig perm = addPermission(config, PermissionType.MAP, "test", "dev");
        perm.addAction(ActionConstants.ACTION_ALL);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            IMap map = client.getMap("test");
            map.put("1", "A");
            map.get("1");
            map.lock("1");
            map.unlock("1");
            map.destroy();
        } finally {
            client.shutdown();
        }
    }

    @Test(expected = RuntimeException.class)
    public void testMapPermissionActions() {
        final Config config = createConfig();
        addPermission(config, PermissionType.MAP, "test", "dev")
                .addAction(ActionConstants.ACTION_PUT)
                .addAction(ActionConstants.ACTION_READ)
                .addAction(ActionConstants.ACTION_REMOVE);

        factory.newHazelcastInstance(config).getMap("test"); // create map
        HazelcastInstance client = createHazelcastClient();
        try {
            IMap map = client.getMap("test");
            assertNull(map.put("1", "A"));
            assertEquals("A", map.get("1"));
            assertEquals("A", map.remove("1"));
            map.lock("1"); // throw exception
        } finally {
            client.shutdown();
        }
    }

    private HazelcastInstance createHazelcastClient() {
        ClientConfig config = new ClientConfig();
        return factory.newHazelcastClient(config);
    }

    @Test
    public void testQueuePermission() {
        final Config config = createConfig();
        addPermission(config, PermissionType.QUEUE, "test", "dev")
                .addAction(ActionConstants.ACTION_ADD).addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            assertTrue(client.getQueue("test").offer("value"));
        } finally {
            client.shutdown();
        }
    }

    @Test(expected = RuntimeException.class)
    public void testQueuePermissionFail() {
        final Config config = createConfig();
        addPermission(config, PermissionType.QUEUE, "test", "dev");
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            client.getQueue("test").offer("value");
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testLockPermission() {
        final Config config = createConfig();
        addPermission(config, PermissionType.LOCK, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE).addAction(ActionConstants.ACTION_LOCK);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            assertTrue(client.getLock("test").tryLock());
            client.getLock("test").unlock();
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testMapReadPermission_alreadyCreatedMap() {
        final Config config = createConfig();
        addPermission(config, PermissionType.MAP, "test", "dev")
                .addAction(ActionConstants.ACTION_READ);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        instance.getMap("test").put("key", "value");
        HazelcastInstance client = createHazelcastClient();
        try {
            IMap<Object, Object> map = client.getMap("test");
            assertEquals("value", map.get("key"));
        } finally {
            client.shutdown();
        }
    }

    @Test(expected = AccessControlException.class)
    public void testLockPermissionFail() {
        final Config config = createConfig();
        addPermission(config, PermissionType.LOCK, "test", "dev")
                .addAction(ActionConstants.ACTION_LOCK);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            client.getLock("test").unlock();
        } finally {
            client.getLifecycleService().shutdown();
        }
    }

    @Test(expected = AccessControlException.class)
    public void testLockPermissionFail2() {
        final Config config = createConfig();
        addPermission(config, PermissionType.LOCK, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            client.getLock("test").tryLock();
        } finally {
            client.getLifecycleService().shutdown();
        }
    }

    @Test
    public void testExecutorPermission() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE);

        addNonExecutorPermissions(config);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            assertEquals(new Integer(11), client.getExecutorService("test").submit(new DummyCallable()).get());
        } finally {
            client.shutdown();
        }
    }

    @Test(expected = ExecutionException.class)
    public void testExecutorPermissionFail() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            client.getExecutorService("test").submit(new DummyCallable()).get();
        } finally {
            client.shutdown();
        }
    }

    @Test(expected = AccessControlException.class)
    public void testExecutorPermissionFail2() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, "test", "dev");
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            client.getExecutorService("test").submit(new DummyCallable()).get();
        } finally {
            client.shutdown();
        }
    }

    @Test(expected = ExecutionException.class)
    public void testExecutorPermissionFail3() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            client.getExecutorService("test").submit(new DummyCallable()).get();
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testExecutorPermissionFail4() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            assertNull(client.getExecutorService("test").submit(new DummyCallableNewThread()).get());
        } finally {
            client.shutdown();
        }
    }

    static class DummyCallable implements Callable<Integer>, Serializable, HazelcastInstanceAware {
        transient HazelcastInstance hz;

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

            hz.getIdGenerator("id_generator").init(0);
            result += hz.getIdGenerator("id_generator").newId(); // +1

            hz.getLock("lock").lock();
            hz.getLock("lock").unlock();
            result++; // +1

            result += hz.getAtomicLong("atomic_long").incrementAndGet(); // +1

            hz.getCountDownLatch("countdown_latch").trySetCount(2);
            hz.getCountDownLatch("countdown_latch").countDown();
            result += hz.getCountDownLatch("countdown_latch").getCount(); // +1

            hz.getSemaphore("semaphore").init(2);
            hz.getSemaphore("semaphore").acquire();
            result += hz.getSemaphore("semaphore").availablePermits(); // +1

            return result;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            hz = hazelcastInstance;
        }
    }

    static class DummyCallableNewThread implements Callable<Integer>, Serializable, HazelcastInstanceAware {
        transient HazelcastInstance hz;

        public Integer call() throws Exception {
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<Integer> value = new AtomicReference<Integer>();
            new Thread() {
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

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            hz = hazelcastInstance;
        }
    }

    private Config createConfig() {
        final Config config = new XmlConfigBuilder().build();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config, PermissionType type, String name, String principal) {
        PermissionConfig perm = new PermissionConfig(type, name, principal);
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }

    private PermissionConfig addAllPermission(Config config, PermissionType type, String name) {
        return addPermission(config, type, name, null).addAction(ActionConstants.ACTION_ALL);
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
    }

}
