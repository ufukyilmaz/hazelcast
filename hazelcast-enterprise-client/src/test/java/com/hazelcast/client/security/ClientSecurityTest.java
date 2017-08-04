package com.hazelcast.client.security;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.LoginModuleConfig;
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
import com.hazelcast.util.FilteringClassLoader;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import usercodedeployment.IncrementingEntryProcessor;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.Writer;
import java.security.AccessControlException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cache.CacheUtil.getDistributedObjectName;
import static com.hazelcast.config.PermissionConfig.PermissionType.ALL;
import static com.hazelcast.config.PermissionConfig.PermissionType.CACHE;
import static com.hazelcast.config.PermissionConfig.PermissionType.MAP;
import static com.hazelcast.config.PermissionConfig.PermissionType.USER_CODE_DEPLOYMENT;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static java.io.File.createTempFile;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";
    static final String HAZELCAST_END_TAG = "</hazelcast>\n";

    TestHazelcastFactory factory = new TestHazelcastFactory();

    private final String testObjectName = randomString();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test(expected = RuntimeException.class)
    public void testDenyAll() {
        final Config config = createConfig();
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        client.getMap("test").size();
    }

    @Test
    public void testAllowAll() {
        final Config config = createConfig();
        addPermission(config, ALL, "", null);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        client.getMap("test").size();
        client.getMap("test").size();
        client.getMap("test").put("a", "b");
        client.getQueue("Q").poll();
    }

    @Test(expected = RuntimeException.class)
    public void testDenyEndpoint() {
        final Config config = createConfig();
        final PermissionConfig pc = addPermission(config, ALL, "", "dev");
        pc.addEndpoint("10.10.10.*");

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        client.getMap("test").size();
    }

    @Test
    public void testMapAllPermission() {
        final Config config = createConfig();
        PermissionConfig perm = addPermission(config, PermissionType.MAP, "test", "dev");
        perm.addAction(ActionConstants.ACTION_ALL);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        IMap map = client.getMap("test");
        map.put("1", "A");
        map.get("1");
        map.lock("1");
        map.unlock("1");
        map.destroy();
    }

    @Test (expected = AccessControlException.class)
    public void testNewPermissionAtRuntime() {
        final Config config = createConfig();
        PermissionConfig perm = addPermission(config, PermissionType.MAP, "test", "dev");
        perm.addAction(ActionConstants.ACTION_ALL);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        assertEquals(0, client.getMap("test").size());

        Set<PermissionConfig> newPermissions = new HashSet<PermissionConfig>();
        PermissionConfig newPerm = addPermission(config, PermissionType.MAP, "test", "dev");
        perm.addAction(ActionConstants.ACTION_READ);
        newPermissions.add(newPerm);
        instance.getConfig().getSecurityConfig().setClientPermissionConfigs(newPermissions);
        //read-only
        client.getMap("test").put("test", "test");
    }

    @Test
    public void testMapAllPermission_multiplePrincipals_withAllowedOne() {
        String loginUser = "UserA";
        String principal = "UserA,UserB";

        final Config config = createConfig(loginUser);

        PermissionConfig perm = addPermission(config, PermissionType.MAP, "test", principal);
        perm.addAction(ActionConstants.ACTION_ALL);

        final ClientConfig cc = new ClientConfig();
        cc.setCredentials(new ClientCustomAuthenticationTest.CustomCredentials(loginUser, "", ""));

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(cc);
        IMap map = client.getMap("test");
        map.put("1", "A");
        map.get("1");
        map.lock("1");
        map.unlock("1");
        map.destroy();
    }

    @Test
    public void testMapAllPermission_multiplePrincipals_withAllowedOne_fromXML()
            throws IOException {
        String loginUser = "role1";

        File file = createTempFile("foo", "bar");
        file.deleteOnExit();

        String xml = HAZELCAST_START_TAG
                + "<security enabled=\"true\">"
                    + "<client-permissions>"
                        + "<map-permission name=\"mySecureMap\" principal=\"role1,role2\">"
                            + "<endpoints>"
                                + "<endpoint>10.10.*.*</endpoint>"
                                + "<endpoint>127.0.0.1</endpoint>"
                            + "</endpoints>"
                            + "<actions>"
                                + "<action>put</action>"
                                + "<action>read</action>"
                                + "<action>destroy</action>"
                          + "</actions>"
                        + "</map-permission>"
                    + "</client-permissions>"
                + "</security>"
                + HAZELCAST_END_TAG;
        Writer writer = new PrintWriter(file, "UTF-8");
        writer.write(xml);
        writer.close();

        String path = file.getAbsolutePath();
        Config config = new XmlConfigBuilder(path).build();
        addCustomUserLoginModule(config, loginUser);

        final ClientConfig cc = new ClientConfig();
        cc.setCredentials(new ClientCustomAuthenticationTest.CustomCredentials(loginUser, "", ""));

        // Create from member
        HazelcastInstance member = factory.newHazelcastInstance(config);
        member.getMap("mySecureMap");

        // Check permissions
        HazelcastInstance client = factory.newHazelcastClient(cc);
        IMap map = client.getMap("mySecureMap");
        map.put("1", "A");
        map.get("1");
        map.destroy();
    }

    @Test
    public void testMapAllPermission_wildcardAllPrincipal() {
        String loginUser = "UserA";
        String principal = "*";

        final Config config = createConfig(loginUser);

        PermissionConfig perm = addPermission(config, PermissionType.MAP, "test", principal);
        perm.addAction(ActionConstants.ACTION_ALL);

        final ClientConfig cc = new ClientConfig();
        cc.setCredentials(new ClientCustomAuthenticationTest.CustomCredentials(loginUser, "", ""));

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(cc);
        IMap map = client.getMap("test");
        map.put("1", "A");
        map.get("1");
        map.lock("1");
        map.unlock("1");
        map.destroy();
    }

    @Test
    public void testMapAllPermission_nullPrincipal() {
        String loginUser = "UserA";
        String principal = null;

        final Config config = createConfig(loginUser);

        PermissionConfig perm = addPermission(config, PermissionType.MAP, "test", principal);
        perm.addAction(ActionConstants.ACTION_ALL);

        final ClientConfig cc = new ClientConfig();
        cc.setCredentials(new ClientCustomAuthenticationTest.CustomCredentials(loginUser, "", ""));

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(cc);
        IMap map = client.getMap("test");
        map.put("1", "A");
        map.get("1");
        map.lock("1");
        map.unlock("1");
        map.destroy();
    }

    @Test(expected = AccessControlException.class)
    public void testMapAllPermission_multiplePrincipalWithoutAllowedOne_andMalformedStringEndingWithSeparator() {
        String loginUser = "UserA";
        String principal = "UserB,UserC,";

        final Config config = createConfig(loginUser);

        PermissionConfig perm = addPermission(config, PermissionType.MAP, "test", principal);
        perm.addAction(ActionConstants.ACTION_ALL);

        final ClientConfig cc = new ClientConfig();
        cc.setCredentials(new ClientCustomAuthenticationTest.CustomCredentials(loginUser, "", ""));

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(cc);

        IMap map = client.getMap("test");
        map.put("1", "A");
        map.get("1");
        map.lock("1");
        map.unlock("1");
        map.destroy();
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
        IMap map = client.getMap("test");
        assertNull(map.put("1", "A"));
        assertEquals("A", map.get("1"));
        assertEquals("A", map.remove("1"));
        map.lock("1"); // throw exception
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
        assertTrue(client.getQueue("test").offer("value"));
    }

    @Test(expected = RuntimeException.class)
    public void testQueuePermissionFail() {
        final Config config = createConfig();
        addPermission(config, PermissionType.QUEUE, "test", "dev");
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        client.getQueue("test").offer("value");
    }

    @Test
    public void testLockPermission() {
        final Config config = createConfig();
        addPermission(config, PermissionType.LOCK, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE).addAction(ActionConstants.ACTION_LOCK);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        assertTrue(client.getLock("test").tryLock());
        client.getLock("test").unlock();
    }

    @Test
    public void testMapReadPermission_alreadyCreatedMap() {
        final Config config = createConfig();
        addPermission(config, PermissionType.MAP, "test", "dev")
                .addAction(ActionConstants.ACTION_READ);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        instance.getMap("test").put("key", "value");
        HazelcastInstance client = createHazelcastClient();
        IMap<Object, Object> map = client.getMap("test");
        assertEquals("value", map.get("key"));
    }

    @Test(expected = AccessControlException.class)
    public void testLockPermissionFail() {
        final Config config = createConfig();
        addPermission(config, PermissionType.LOCK, "test", "dev")
                .addAction(ActionConstants.ACTION_LOCK);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        client.getLock("test").unlock();
    }

    @Test(expected = AccessControlException.class)
    public void testLockPermissionFail2() {
        final Config config = createConfig();
        addPermission(config, PermissionType.LOCK, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        client.getLock("test").tryLock();
    }

    @Test
    public void testExecutorPermission() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE);

        addNonExecutorPermissions(config);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        assertEquals(new Integer(11), client.getExecutorService("test").submit(new DummyCallable()).get());
    }

    @Test(expected = ExecutionException.class)
    public void testExecutorPermissionFail() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        client.getExecutorService("test").submit(new DummyCallable()).get();
    }

    @Test(expected = AccessControlException.class)
    public void testExecutorPermissionFail2() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, "test", "dev");
        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        client.getExecutorService("test").submit(new DummyCallable()).get();
    }

    @Test
    public void testExecutorPermissionFail3() throws InterruptedException, ExecutionException {
        final Config config = createConfig();
        addPermission(config, PermissionType.EXECUTOR_SERVICE, "test", "dev")
                .addAction(ActionConstants.ACTION_CREATE);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        assertNull(client.getExecutorService("test").submit(new DummyCallableNewThread()).get());
    }

    @Test
    public void testCacheAllPermission() {
        final Config config = createConfig();
        PermissionConfig perm = addPermission(config, CACHE, getDistributedObjectName(testObjectName), "dev");
        perm.addAction(ActionConstants.ACTION_ALL);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        Cache<String, String> cache = createCache(client, testObjectName, true);
        cache.put("1", "A");
        cache.get("1");
        cache.unwrap(ICache.class).size();
        cache.unwrap(ICache.class).destroy();
    }

    @Test
    public void testCache_someActionsPermitted() {
        final Config config = createConfig();
        addPermission(config, CACHE, getDistributedObjectName(testObjectName), "dev")
                .addAction(ActionConstants.ACTION_CREATE)
                .addAction(ActionConstants.ACTION_PUT)
                .addAction(ActionConstants.ACTION_READ)
                .addAction(ActionConstants.ACTION_REMOVE);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        Cache<String, String> cache = createCache(client, testObjectName, true);
        cache.put("1", "A");
        cache.get("1");
        cache.unwrap(ICache.class).size();
        expectedException.expect(AccessControlException.class);
        cache.unwrap(ICache.class).destroy();
    }

    @Test
    public void testCache_accessAlreadyCreatedCache() {
        final Config config = createConfig();
        addPermission(config, CACHE, getDistributedObjectName(testObjectName), "dev")
                .addAction(ActionConstants.ACTION_READ);

        HazelcastInstance member = factory.newHazelcastInstance(config);
        Cache<String, String> memberCache = createCache(member, testObjectName, false);
        memberCache.put("1", "A");

        HazelcastInstance client = createHazelcastClient();
        Cache<String, String> cache = getCacheManager(client, true).getCache(testObjectName);
        assertEquals("A", cache.get("1"));
        expectedException.expect(AccessControlException.class);
        cache.put("1", "B");
    }

    @Test(expected = IllegalStateException.class)
    public void testUserCodeDeploymentNotAllowed() {
        testUserCodeDeployment(ActionConstants.ACTION_ADD);
    }

    @Test
    public void testUserCodeDeploymentDeployAllowed() {
        testUserCodeDeployment(ActionConstants.ACTION_USER_CODE_DEPLOY);
    }

    @Test
    public void testUserCodeDeploymentDeployAllAllowed() {
        testUserCodeDeployment(ActionConstants.ACTION_ALL);
    }

    public void testUserCodeDeployment(String actionType) {
        Config config = createConfig();
        addPermission(config, USER_CODE_DEPLOYMENT, null, "dev")
                .addAction(actionType);
        addPermission(config, MAP, "*", "dev")
                .addAction(ActionConstants.ACTION_ALL);
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        config.setClassLoader(filteringCL);
        config.getUserCodeDeploymentConfig()
                .setEnabled(true);
        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("usercodedeployment.IncrementingEntryProcessor");
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IncrementingEntryProcessor incrementingEntryProcessor = new IncrementingEntryProcessor();
        IMap<Integer, Integer> map = client.getMap(randomName());
        map.put(1, 1);
        map.executeOnEntries(incrementingEntryProcessor);
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

    private Config createConfig(String allowedLoginUsername) {
        final Config config = createConfig();
        addCustomUserLoginModule(config, allowedLoginUsername);
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

    private void addCustomUserLoginModule(Config config, String allowedLoginUsername) {
        Properties prop = new Properties();
        prop.setProperty("username", allowedLoginUsername);
        prop.setProperty("key1", "");
        prop.setProperty("key2", "");

        SecurityConfig secCfg = config.getSecurityConfig();

        secCfg.addClientLoginModuleConfig(
                new LoginModuleConfig()
                        .setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED)
                        .setClassName(ClientCustomAuthenticationTest.CustomLoginModule.class.getName())
                        .setProperties(prop));
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

    // create a Cache<String, String> on the default CacheManager backed by the given HazelcastInstance
    private Cache<String, String> createCache(HazelcastInstance hazelcastInstance, String cacheName, boolean client) {
        CacheManager defaultCacheManager = getCacheManager(hazelcastInstance, client);
        return defaultCacheManager.createCache(cacheName, new MutableConfiguration<String, String>());
    }

    // get a CacheManager backed by the given HazelcastInstance
    private CacheManager getCacheManager(HazelcastInstance hazelcastInstance, boolean client) {
        CachingProvider cachingProvider;
        if (client) {
            cachingProvider = Caching.getCachingProvider("com.hazelcast.client.cache.impl.HazelcastClientCachingProvider");
        } else {
            cachingProvider = Caching.getCachingProvider("com.hazelcast.cache.impl.HazelcastServerCachingProvider");
        }
        return cachingProvider.getCacheManager(null, null,
                HazelcastCachingProvider.propertiesByInstanceItself(hazelcastInstance));
    }

}
