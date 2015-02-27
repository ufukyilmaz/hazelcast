package com.hazelcast.client.security;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.security.permission.ActionConstants;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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
public class ClientSecurityTest {

    @BeforeClass
    @AfterClass
    public static void cleanupClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(expected = RuntimeException.class)
    public void testDenyAll() {
        final Config config = createConfig();
        Hazelcast.newHazelcastInstance(config);
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

        Hazelcast.newHazelcastInstance(config);
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

        Hazelcast.newHazelcastInstance(config);
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

        Hazelcast.newHazelcastInstance(config);
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

        Hazelcast.newHazelcastInstance(config).getMap("test"); // create map
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
        ClientConfig config = new ClientConfig().addAddress("127.0.0.1");
        return HazelcastClient.newHazelcastClient(config);
    }

    @Test
    public void testQueuePermission() {
        final Config config = createConfig();
        addPermission(config, PermissionType.QUEUE, "test", "dev")
                .addAction(ActionConstants.ACTION_ADD).addAction(ActionConstants.ACTION_CREATE);
        Hazelcast.newHazelcastInstance(config);
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
        Hazelcast.newHazelcastInstance(config);
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
        Hazelcast.newHazelcastInstance(config);
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
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
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
        Hazelcast.newHazelcastInstance(config);
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
        Hazelcast.newHazelcastInstance(config);
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

        addPermission(config, PermissionType.LIST, "list", null)
                .addAction(ActionConstants.ACTION_ADD).addAction(ActionConstants.ACTION_CREATE)
                .addAction(ActionConstants.ACTION_READ);

        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        try {
            assertEquals(new Integer(1), client.getExecutorService("test").submit(new DummyCallable()).get());
        } finally {
            client.shutdown();
        }
    }

    @Test(expected = ExecutionException.class)
    public void testExecutorPermissionFail() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE);
        Hazelcast.newHazelcastInstance(config);
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
        Hazelcast.newHazelcastInstance(config);
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

        Hazelcast.newHazelcastInstance(config);
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

        Hazelcast.newHazelcastInstance(config);
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
            hz.getList("list").add("value");
            return hz.getList("list").size();
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
}
