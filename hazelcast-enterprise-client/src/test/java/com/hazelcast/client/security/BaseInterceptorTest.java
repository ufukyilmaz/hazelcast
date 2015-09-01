package com.hazelcast.client.security;


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
import com.hazelcast.query.Predicate;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.Parameters;
import com.hazelcast.security.SecurityInterceptor;
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

    String getObjectType() {
        return null;
    }

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

        String objectType;
        String objectName;
        String methodName;
        Object[] params;

        @Override
        public void before(Credentials credentials, String objectType, String objectName,
                           String methodName, Parameters parameters) throws AccessControlException {
            this.objectType = objectType;
            this.objectName = objectName;
            this.methodName = methodName;
            final int length = parameters.length();
            params = new Object[length];
            for (int i = 0; i < length; i++) {
                params[i] = parameters.get(i);
            }
        }

        @Override
        public void after(Credentials credentials, String objectType, String objectName,
                          String methodName, Parameters parameters) {
        }

        void reset() {
            methodName = null;
            params = null;
        }

        void assertMethod(String objectType, String objectName, String methodName, Object... params) {
            assertEquals(objectType, this.objectType);
            assertEquals(objectName, this.objectName);
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
        return randomInt(1000) + 1;
    }

    public static int randomInt(int max) {
        return new Random(System.currentTimeMillis()).nextInt(max);
    }

    static class DummyPredicate implements Predicate {

        long i;

        DummyPredicate() {
        }

        DummyPredicate(long i) {
            this.i = i;
        }

        @Override
        public boolean apply(final Map.Entry entry) {
            return false;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final DummyPredicate that = (DummyPredicate) o;

            if (i != that.i) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return (int) (i ^ (i >>> 32));
        }
    }

    static class DummyFunction implements IFunction {

        long i;

        DummyFunction() {

        }

        DummyFunction(long i) {
            this.i = i;
        }

        @Override
        public Object apply(final Object o) {
            return i;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final DummyFunction that = (DummyFunction) o;

            if (i != that.i) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return (int) (i ^ (i >>> 32));
        }
    }
}
