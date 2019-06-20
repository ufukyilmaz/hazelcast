package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.ClockProperties;
import com.hazelcast.util.FilteringClassLoader;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.GroupConfig.DEFAULT_GROUP_NAME;
import static com.hazelcast.config.GroupConfig.DEFAULT_GROUP_PASSWORD;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;

/**
 * Tests whether a huge difference in cluster clocks causes WAN operations
 * to fail on the target cluster because of invocation call timeout.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class WanReplicationClockDiffTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overridePropertyRule = set(HAZELCAST_TEST_USE_NETWORK, "true");

    public static final String WAN_REPLICATION_SCHEME = "wanReplication";
    public static final String MAP_NAME = "mappy";
    protected Object isolatedNode;

    @After
    public void tearDown() {
        HazelcastInstanceFactory.terminateAll();
        shutdownIsolatedNode();
        resetClock();
    }

    @Test
    public void mapIsReplicatedWhenClusterClockDifferenceIsExtreme() {
        int entryCount = 100;

        setClockOffset(TimeUnit.MINUTES.toMillis(-60));
        HazelcastInstance source = startSourceNode();
        resetClock();

        startIsolatedTargetNode();

        IMap<Object, Object> sourceMap = source.getMap(MAP_NAME);
        for (int i = 0; i < entryCount; i++) {
            sourceMap.put(i, i);
        }

        assertEqualsEventually(new Callable<Object>() {
            @Override
            public Integer call() throws Exception {
                return getTargetClusterMapSize(MAP_NAME);
            }
        }, entryCount);
    }

    private Integer getTargetClusterMapSize(String mapName) throws Exception {
        if (isolatedNode == null) {
            throw new IllegalStateException("There is no isolated node running!");
        }

        Object map = invokeReflectively(isolatedNode, "getMap", String.class, mapName);
        Method sizeMethod = Map.class.getDeclaredMethod("size");
        Integer size = (Integer) sizeMethod.invoke(map);
        return size;
    }

    private void startIsolatedTargetNode() {
        if (isolatedNode != null) {
            throw new IllegalStateException("There is already an isolated node running!");
        }
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        try {
            FilteringClassLoader cl = new FilteringClassLoader(Collections.<String>emptyList(), "com.hazelcast");
            thread.setContextClassLoader(cl);

            Class<?> configClazz = cl.loadClass("com.hazelcast.config.Config");
            Object config = configClazz.newInstance();
            invokeReflectively(config, "setClassLoader", ClassLoader.class, cl);
            Object networkConfig = invokeReflectively(config, "getNetworkConfig");
            Object joinConfig = invokeReflectively(networkConfig, "getJoin");
            Object multicastConfig = invokeReflectively(joinConfig, "getMulticastConfig");
            invokeReflectively(multicastConfig, "setEnabled", boolean.class, false);

            Object tcpIpConfig = invokeReflectively(joinConfig, "getTcpIpConfig");
            invokeReflectively(tcpIpConfig, "setEnabled", boolean.class, true);
            invokeReflectively(tcpIpConfig, "addMember", String.class, "127.0.0.1");

            Class<?> hazelcastClazz = cl.loadClass("com.hazelcast.core.Hazelcast");
            Method newHazelcastInstance = hazelcastClazz.getDeclaredMethod("newHazelcastInstance", configClazz);
            isolatedNode = newHazelcastInstance.invoke(hazelcastClazz, config);
        } catch (Exception e) {
            throw new RuntimeException("Could not start isolated Hazelcast instance", e);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    private static void setClockOffset(long offset) {
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET, String.valueOf(offset));
    }

    protected static void resetClock() {
        System.clearProperty(ClockProperties.HAZELCAST_CLOCK_IMPL);
        System.clearProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET);
    }

    private Object invokeReflectively(Object target, String methodName) throws Exception {
        return invokeReflectively(target, methodName, null, null);
    }

    private Object invokeReflectively(Object target, String methodName, Class paramType, Object param) throws Exception {
        Class<?> targetClass = target.getClass();
        Method method = paramType != null
                ? targetClass.getDeclaredMethod(methodName, paramType)
                : targetClass.getDeclaredMethod(methodName);
        return paramType != null ? method.invoke(target, param) : method.invoke(target);
    }

    private void shutdownIsolatedNode() {
        if (isolatedNode == null) {
            return;
        }
        try {
            Class<?> instanceClass = isolatedNode.getClass();
            Method method = instanceClass.getMethod("shutdown");
            method.invoke(isolatedNode);
            isolatedNode = null;
        } catch (Exception e) {
            throw new RuntimeException("Could not start shutdown Hazelcast instance", e);
        }
    }

    private HazelcastInstance startSourceNode() {
        WanPublisherConfig publisherConfig = new WanPublisherConfig();
        publisherConfig.setGroupName(DEFAULT_GROUP_NAME);
        publisherConfig.setClassName(WanBatchReplication.class.getName());
        Map<String, Comparable> props = publisherConfig.getProperties();
        props.put(WanReplicationProperties.GROUP_PASSWORD.key(), DEFAULT_GROUP_PASSWORD);
        props.put(WanReplicationProperties.ENDPOINTS.key(), "127.0.0.1:5701");

        Config config = smallInstanceConfig();

        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig()
                  .setEnabled(false);
        joinConfig.getTcpIpConfig()
                  .setEnabled(true)
                  .addMember("127.0.0.1");

        config.getGroupConfig().setName("B");
        config.setInstanceName("confB-" + UUID.randomUUID() + "-");
        config.getNetworkConfig().setPort(5801);
        config.addWanReplicationConfig(new WanReplicationConfig().setName(WAN_REPLICATION_SCHEME)
                                                                 .addWanPublisherConfig(publisherConfig));
        WanReplicationRef wanReplicationRef = new WanReplicationRef()
                .setName(WAN_REPLICATION_SCHEME)
                .setMergePolicy(PassThroughMergePolicy.class.getName());
        config.getMapConfig("default").setWanReplicationRef(wanReplicationRef);
        return Hazelcast.newHazelcastInstance(config);
    }
}
