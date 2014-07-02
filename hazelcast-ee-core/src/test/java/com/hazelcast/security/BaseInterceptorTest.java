package com.hazelcast.security;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.MapLoader;
<<<<<<< HEAD
=======
import com.hazelcast.map.MapService;
import com.hazelcast.query.Predicate;
>>>>>>> fad95c983932ce93541a9eb5006a355fd27765c1
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.security.AccessControlException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class BaseInterceptorTest {

    static HazelcastInstance instance;
    static HazelcastInstance client;
    static TestSecurityInterceptor interceptor;

    @BeforeClass
    public static void cleanupClass() {
        interceptor = new TestSecurityInterceptor();
        final Config config = createConfig(interceptor);
        instance = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient();
    }

    @Before
    public void reset() {
        interceptor.reset();
    }

    @AfterClass
    public static void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    abstract String getServiceName();

    private static Config createConfig(TestSecurityInterceptor interceptor) {
        final Config config = new Config();
        PermissionConfig perm = new PermissionConfig(PermissionConfig.PermissionType.ALL, "", null);
        final SecurityConfig securityConfig = config.getSecurityConfig();
        securityConfig.setEnabled(true).addClientPermissionConfig(perm);

        final SecurityInterceptorConfig interceptorConfig = new SecurityInterceptorConfig();
        interceptorConfig.setImplementation(interceptor);
        securityConfig.addSecurityInterceptorConfig(interceptorConfig);

        final MapConfig mapConfig = config.getMapConfig("loadAll*");
        final MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        final DummyLoader dummyLoader = new DummyLoader();
        mapStoreConfig.setImplementation(dummyLoader);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return config;
    }

    static class TestSecurityInterceptor implements SecurityInterceptor {

        String serviceName;
        String methodName;
        Object[] params;

        @Override
        public void before(final Credentials credentials, final String serviceName, final String methodName, final Parameters parameters) throws AccessControlException {
            this.serviceName = serviceName;
            this.methodName = methodName;
            final int length = parameters.length();
            params = new Object[length];
            for (int i = 0; i < length; i++) {
                params[i] = parameters.get(i);
            }
        }

        @Override
        public void after(final Credentials credentials, final String serviceName, final String methodName, final Parameters parameters) {
        }

        void reset() {
            methodName = null;
            params = null;
        }

        void assertMethod(String serviceName, String methodName, Object... params) {
            assertEquals(serviceName, this.serviceName);
            assertEquals(methodName, this.methodName);
            int len = params.length;
            assertNotNull(this.params);
            assertEquals(len, this.params.length);
            for (int i = 0; i < len; i++) {
                if (params[i] instanceof Collection) {
                    assertCollection((Collection) params[i], this.params[i]);
                } else if (params[i] instanceof Map) {
                    assertMap((Map) params[i], this.params[i]);
                } else {
                    assertEquals(params[i], this.params[i]);
                }
            }
        }

        private void assertCollection(Collection collection, Object otherParam) {
            assertTrue(otherParam instanceof Collection);
            Collection otherCollection = (Collection) otherParam;
            assertEquals(collection.size(), otherCollection.size());
            for (Object o : collection) {
                assertTrue(otherCollection.contains(o));
            }
        }

        private void assertMap(Map<Object, Object> map, Object otherParam) {
            assertTrue(otherParam instanceof Map);
            Map otherMap = (Map) otherParam;
            assertEquals(map.size(), otherMap.size());
            for (Map.Entry entry : map.entrySet()) {
                assertEquals(entry.getValue(), otherMap.get(entry.getKey()));
            }
        }

    }

    static class DummyLoader implements MapLoader {

        static final HashMap map = new HashMap();

        static {
            map.put(randomString(), randomString());
            map.put(randomString(), randomString());
            map.put(randomString(), randomString());
        }

        DummyLoader() {
        }

        @Override
        public Object load(final Object key) {
            return map.get(key);
        }

        @Override
        public Map loadAll(final Collection keys) {
            final HashMap hashMap = new HashMap();
            for (Object key : keys) {
                final Object value = map.get(key);
                if (value != null) {
                    hashMap.put(key, value);
                }
            }
            return hashMap;
        }

        @Override
        public Set loadAllKeys() {
            return map.keySet();
        }
    }

    public static String randomString() {
        return UUID.randomUUID().toString();
    }

    public static long randomLong() {
        return randomInt(1000);
    }

    public static int randomInt(int max) {
        return new Random(System.currentTimeMillis()).nextInt(max);
    }

}
