package com.hazelcast.client.security;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.ICache;
import com.hazelcast.client.cache.jsr.JsrClientTestUtil;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import java.security.AccessControlException;

import static com.hazelcast.cache.CacheUtil.getDistributedObjectName;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class CacheSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String testObjectName = randomString();

    @BeforeClass
    public static void initJCache() {
        JsrClientTestUtil.setup();
    }

    @AfterClass
    public static void cleanupJCache() {
        JsrClientTestUtil.cleanup();
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testCacheAllPermission() {
        final Config config = createConfig();
        PermissionConfig perm = addPermission(config, getDistributedObjectName(testObjectName));
        perm.addAction(ActionConstants.ACTION_ALL);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        Cache<String, String> cache = createCache(client, testObjectName, true);
        cache.put("1", "A");
        cache.get("1");
        cache.unwrap(ICache.class).size();
        cache.unwrap(ICache.class).destroy();
    }

    @Test
    public void testCache_someActionsPermitted() {
        final Config config = createConfig();
        addPermission(config, getDistributedObjectName(testObjectName))
                .addAction(ActionConstants.ACTION_CREATE)
                .addAction(ActionConstants.ACTION_PUT)
                .addAction(ActionConstants.ACTION_READ)
                .addAction(ActionConstants.ACTION_REMOVE);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
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
        addPermission(config, getDistributedObjectName(testObjectName))
                .addAction(ActionConstants.ACTION_READ);

        HazelcastInstance member = factory.newHazelcastInstance(config);
        Cache<String, String> memberCache = createCache(member, testObjectName, false);
        memberCache.put("1", "A");

        HazelcastInstance client = factory.newHazelcastClient();
        Cache<String, String> cache = getCacheManager(client, true).getCache(testObjectName);
        assertEquals("A", cache.get("1"));
        expectedException.expect(AccessControlException.class);
        cache.put("1", "B");
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config, String name) {
        PermissionConfig perm = new PermissionConfig(PermissionType.CACHE, name, "dev");
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
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
