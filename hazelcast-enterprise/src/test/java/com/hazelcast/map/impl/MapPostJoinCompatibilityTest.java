package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.query.impl.IndexInfo;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.test.starter.ReflectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({CompatibilityTest.class})
public class MapPostJoinCompatibilityTest extends HazelcastTestSupport {

    private static String MAP_NAME = "MapPostJoinCompatibilityTest";

    @Test
    public void assert_compat_38_and_39_runtimeIndexAddedIn38() {
        // GIVEN CONFIG
        String[] versions = new String[]{"3.8", "3.9-SNAPSHOT"};
        TestHazelcastInstanceFactory factory = new CompatibilityTestHazelcastInstanceFactory(versions);

        Config config = getHDConfig();
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

        HazelcastInstance[] instances = new HazelcastInstance[versions.length];
        instances[0] = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(1, instances[0]);

        IndexInfo power = index("power", false);
        instances[0].getMap(MAP_NAME).addIndex(power.getAttributeName(), power.isOrdered());

        // GIVEN VALUES
        IMap map = instances[0].getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            map.put(i, new Car(i));
        }

        instances[1] = factory.newHazelcastInstance(config);
        assertIndexesEqualEventually(instances[1], power);

        // TEAR-DOWN
        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }

    @Test
    public void assert_compat_38_and_39_indexSync() {
        // GIVEN CONFIG
        String[] versions = new String[]{"3.8", "3.9-SNAPSHOT", "3.9-SNAPSHOT"};
        TestHazelcastInstanceFactory factory = new CompatibilityTestHazelcastInstanceFactory(versions);

        Config config = getHDConfig();
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

        HazelcastInstance[] instances = new HazelcastInstance[versions.length];
        instances[0] = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(1, instances[0]);

        IndexInfo power = index("power", false);
        instances[0].getMap(MAP_NAME).addIndex(power.getAttributeName(), power.isOrdered());

        // GIVEN VALUES
        IMap map = instances[0].getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            map.put(i, new Car(i));
        }

        instances[1] = factory.newHazelcastInstance(config);
        instances[2] = factory.newHazelcastInstance(config);
        assertIndexesEqualEventually(instances[1], power);
        assertIndexesEqualEventually(instances[2], power);

        instances[0].shutdown();

        assertEquals(Versions.V3_8, instances[1].getCluster().getClusterVersion());

        boolean success = false;
        while (!success) {
            try {
                instances[1].getCluster().changeClusterVersion(Versions.V3_9);
                success = true;
            } catch (IllegalStateException ex) {
                if (!ex.getMessage().contains("Still have pending migration tasks")) {
                    throw ex;
                }
            }
        }

        assertEquals(Versions.V3_9, instances[1].getCluster().getClusterVersion());

        // TEAR-DOWN
        for (int i = 1; i <= 2; i++) {
            instances[i].shutdown();
        }
    }

    private Map<String, Boolean> getIndexDefinitions(HazelcastInstance instance) {
        try {
            Object h = ReflectionUtils.getFieldValueReflectively(instance, "h");
            Object delegate = ReflectionUtils.getFieldValueReflectively(h, "delegate");
            Object proxy = ReflectionUtils.getFieldValueReflectively(delegate, "original");
            Object node = ReflectionUtils.getFieldValueReflectively(proxy, "node");
            Object nodeEngine = ReflectionUtils.getFieldValueReflectively(node, "nodeEngine");
            Object mapService = callMethod("getService", nodeEngine, MapService.SERVICE_NAME);
            Object mapServiceContext = ReflectionUtils.getFieldValueReflectively(mapService, "mapServiceContext");
            Object mapContainer = callMethod("getMapContainer", mapServiceContext, MAP_NAME);
            return (Map<String, Boolean>) callMethod("getIndexDefinitions", mapContainer);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    static Object callMethod(String methodName, Object receiver, Object... params) {
        Class[] classParams = new Class[params.length];
        int i = 0;
        for (Object param : params) {
            classParams[i++] = param.getClass();
        }
        try {
            Class receiverClass = receiver.getClass();
            Method method = null;
            while (receiverClass != null) {
                try {
                    method = receiverClass.getDeclaredMethod(methodName, classParams);
                    if (method != null) {
                        break;
                    }
                } catch (Exception ex) {
                    receiverClass = receiverClass.getSuperclass();
                }
            }
            method.setAccessible(true);
            return method.invoke(receiver, params);
        } catch (Throwable e) {
            throw new RuntimeException(e + " for method " + methodName);
        }
    }

    private void assertIndexesEqualEventually(final HazelcastInstance instance, IndexInfo... indexInfos) {
        final Map<String, Boolean> indexes = indexes(indexInfos);
        assertEqualsEventually(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return getIndexDefinitions(instance).equals(indexes);
            }
        }, true);
    }

    private IndexInfo index(String path, boolean ordered) {
        return new IndexInfo(path, ordered);
    }

    private Map<String, Boolean> indexes(IndexInfo... indexInfos) {
        Map<String, Boolean> indexes = new HashMap<String, Boolean>();
        for (IndexInfo indexInfo : indexInfos) {
            indexes.put(indexInfo.getAttributeName(), indexInfo.isOrdered());
        }
        return indexes;
    }

    public static class Car implements ICar, Serializable {
        private final long power;

        public Car(long power) {
            this.power = power;
        }

        @Override
        public long getPower() {
            return power;
        }
    }

    public interface ICar {
        long getPower();
    }
}
