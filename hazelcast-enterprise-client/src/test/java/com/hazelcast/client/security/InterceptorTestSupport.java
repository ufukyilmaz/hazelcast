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
import com.hazelcast.core.MapLoader;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.Parameters;
import com.hazelcast.security.SecurityInterceptor;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.RandomPicker;
import org.junit.After;
import org.junit.Before;

import java.security.AccessControlException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public abstract class InterceptorTestSupport extends HazelcastTestSupport {

    public static final Object SKIP_COMPARISON_OBJECT = new Object();

    TestHazelcastFactory factory = new TestHazelcastFactory();
    TestSecurityInterceptor interceptor = new TestSecurityInterceptor();
    HazelcastInstance instance;
    HazelcastInstance client;

    @Before
    public void before() {
        Config config = createConfig(interceptor);
        instance = factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient();
    }

    @After
    public void check() {
        try {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertTrue(interceptor.success);
                }
            }, 20);
        } finally {
            factory.terminateAll();
        }
    }

    String getObjectType() {
        return null;
    }

    private static Config createConfig(TestSecurityInterceptor interceptor) {
        Config config = new Config();
        PermissionConfig perm = new PermissionConfig(PermissionConfig.PermissionType.ALL, "", null);
        SecurityConfig securityConfig = config.getSecurityConfig();
        securityConfig.setEnabled(true).addClientPermissionConfig(perm);

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

        String expectedObjectType;
        String expectedObjectName;
        String expectedMethodName;
        Object[] expectedParams;
        volatile boolean success;

        @Override
        public void before(Credentials credentials, String objectType, String objectName,
                           String methodName, Parameters parameters) throws AccessControlException {
            if (!checkEqual(expectedObjectType, objectType)) {
                return;
            }
            if (!checkEqual(expectedObjectName, objectName)) {
                return;
            }
            if (!checkEqual(expectedMethodName, methodName)) {
                return;
            }
            if (parameters.length() != expectedParamLength()) {
                return;
            }
            synchronized (this) {
                int length = expectedParamLength();
                for (int i = 0; i < length; i++) {
                    Object expectedParam = expectedParams[i];
                    Object actualParam = parameters.get(i);
                    if (expectedParam instanceof Map && actualParam instanceof Map) {
                        Map expectedMap = (Map) expectedParam;
                        Map<Object, Object> actualMap = (Map<Object, Object>) actualParam;
                        for (Map.Entry o : actualMap.entrySet()) {
                            if (!o.getValue().equals(expectedMap.remove(o.getKey()))) {
                                return;
                            }
                        }
                        if (!expectedMap.isEmpty()) {
                            return;
                        }
                    } else if (expectedParam instanceof Collection && actualParam instanceof Collection) {
                        Collection expectedCollection = (Collection) expectedParam;
                        Collection actualCollection = (Collection) actualParam;
                        expectedCollection.removeAll(actualCollection);
                        if (!expectedCollection.isEmpty()) {
                            return;
                        }
                    } else if (!checkEqual(expectedParam, actualParam)) {
                        return;
                    }

                }
            }
            success = true;
        }

        private int expectedParamLength() {
            return expectedParams == null ? 0 : expectedParams.length;
        }

        @Override
        public void after(Credentials credentials, String objectType, String objectName,
                          String methodName, Parameters parameters) {
        }

        void setExpectation(String objectType, String objectName, String methodName, Object... params) {
            this.expectedObjectType = objectType;
            this.expectedObjectName = objectName;
            this.expectedMethodName = methodName;
            this.expectedParams = params;
        }

        private boolean checkEqual(Object expected, Object actual) {
            if (expected == null && actual == null) {
                return true;
            }
            if (expected != null && expected.equals(actual)) {
                return true;
            }
            if (expected == SKIP_COMPARISON_OBJECT) {
                return true;
            }
            return false;
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
