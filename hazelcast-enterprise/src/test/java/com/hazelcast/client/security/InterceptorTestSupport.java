package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.map.MapLoader;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.Parameters;
import com.hazelcast.security.SecurityInterceptor;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.internal.util.RandomPicker;
import org.junit.After;
import org.junit.Before;

import java.security.AccessControlException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public abstract class InterceptorTestSupport extends HazelcastTestSupport {

    public static final Object SKIP_COMPARISON_OBJECT = new Object();

    TestHazelcastFactory factory = new TestHazelcastFactory();
    TestSecurityInterceptor interceptor = new TestSecurityInterceptor();
    HazelcastInstance instance;
    HazelcastInstance client;

    @Before
    public void before() {
        Config config = createConfig();
        instance = factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient();
    }

    @After
    public void check() {
        try {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    if (interceptor.success) {
                        return;
                    }
                    if (interceptor.failure != null) {
                        throw interceptor.failure;
                    }
                }
            }, 20);
        } finally {
            factory.terminateAll();
        }
    }

    String getObjectType() {
        return null;
    }

    Config createConfig() {
        Config config = new Config();
        PermissionConfig perm = new PermissionConfig(PermissionConfig.PermissionType.ALL, "", null);
        SecurityConfig securityConfig = config.getSecurityConfig();
        securityConfig.setEnabled(true)
            .addClientPermissionConfig(perm);

        SecurityInterceptorConfig interceptorConfig = new SecurityInterceptorConfig();
        interceptorConfig.setImplementation(interceptor);
        securityConfig.addSecurityInterceptorConfig(interceptorConfig);

        MapConfig mapConfig = config.getMapConfig("loadAll*");
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        DummyLoader dummyLoader = new DummyLoader();
        mapStoreConfig.setImplementation(dummyLoader);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return config;
    }

    static class TestSecurityInterceptor implements SecurityInterceptor {

        private volatile InterceptorExpectation expectation;
        private volatile boolean success;
        private volatile AssertionError failure;

        @Override
        public void before(Credentials credentials, String objectType, String objectName,
                           String methodName, Parameters parameters) throws AccessControlException {
            if (expectation == null) {
                return;
            }

            try {
                checkExpectation(objectType, objectName, methodName, parameters);
                success = true;
            } catch (AssertionError e) {
                failure = e;
            } catch (Throwable e) {
                failure = new AssertionError(e);
            }
        }

        private void checkExpectation(String objectType, String objectName, String methodName, Parameters parameters) {
            checkEqual(expectation.objectType, objectType);
            checkEqual(expectation.objectName, objectName);
            checkEqual(expectation.methodName, methodName);
            assertEquals(expectation.paramLength(), parameters.length());

            synchronized (this) {
                int length = expectation.paramLength();
                for (int i = 0; i < length; i++) {
                    Object expectedParam = expectation.params[i];
                    Object actualParam = parameters.get(i);
                    if (expectedParam instanceof Map && actualParam instanceof Map) {
                        Map<Object, Object> expectedMap = (Map) expectedParam;
                        Map<Object, Object> actualMap = (Map<Object, Object>) actualParam;
                        for (Map.Entry o : actualMap.entrySet()) {
                            checkEqual(expectedMap.remove(o.getKey()), o.getValue());
                        }
                        assertThat(expectedMap.entrySet(), empty());
                    } else if (expectedParam instanceof Collection && actualParam instanceof Collection) {
                        Collection<Object> expectedCollection = (Collection) expectedParam;
                        Collection<Object> actualCollection = (Collection) actualParam;
                        expectedCollection.removeAll(actualCollection);
                        assertThat(expectedCollection, empty());
                    } else {
                        checkEqual(expectedParam, actualParam);
                    }
                }
            }
        }

        @Override
        public void after(Credentials credentials, String objectType, String objectName,
                          String methodName, Parameters parameters) {
        }

        void setExpectation(String objectType, String objectName, String methodName, Object... params) {
           this.expectation = new InterceptorExpectation(objectType, objectName, methodName, params);
        }

        private void checkEqual(Object expected, Object actual) {
            if (expected == null && actual == null) {
                return;
            }
            if (expected != null && expected.equals(actual)) {
                return;
            }
            if (expected == SKIP_COMPARISON_OBJECT) {
                return;
            }
            throw new AssertionError("Expected: " + expected + ", Actual: " + actual);
        }
    }

    private static class InterceptorExpectation {
        final String objectType;
        final String objectName;
        final String methodName;
        final Object[] params;

        private InterceptorExpectation(String objectType, String objectName,
                String methodName,
                Object[] params) {
            this.objectType = objectType;
            this.objectName = objectName;
            this.methodName = methodName;
            this.params = params;
        }

        int paramLength() {
            return params == null ? 0 : params.length;
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
        public Object load(Object key) {
            return map.get(key);
        }

        @Override
        public Map loadAll(Collection keys) {
            HashMap hashMap = new HashMap();
            for (Object key : keys) {
                Object value = map.get(key);
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

    public static int randomInt(int min, int max) {
        return RandomPicker.getInt(min, max);
    }

    public static int randomInt(int max) {
        return RandomPicker.getInt(max);
    }

    static class DummyPredicate implements Predicate {

        long i;

        DummyPredicate() {
        }

        DummyPredicate(long i) {
            this.i = i;
        }

        @Override
        public boolean apply(Map.Entry entry) {
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DummyPredicate that = (DummyPredicate) o;
            if (i != that.i) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return (int) (i ^ (i >>> 32));
        }
    }

    @SuppressWarnings("unused")
    static class DummyFunction implements IFunction {

        long i;

        DummyFunction() {
        }

        DummyFunction(long i) {
            this.i = i;
        }

        @Override
        public Object apply(Object o) {
            return i;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DummyFunction that = (DummyFunction) o;
            if (i != that.i) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return (int) (i ^ (i >>> 32));
        }
    }
}
