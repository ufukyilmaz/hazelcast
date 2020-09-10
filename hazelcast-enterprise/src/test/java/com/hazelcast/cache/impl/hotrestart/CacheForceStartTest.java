package com.hazelcast.cache.impl.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.cache.jsr.JsrTestUtil.assertNoMBeanLeftovers;
import static com.hazelcast.test.Accessors.getNode;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheForceStartTest extends AbstractCacheHotRestartTest {

    private static final int CLUSTER_SIZE = 3;

    @Parameters(name = "memoryFormat:{0} fsync:{2} encrypted:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, false, false, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, false, false, false},
                {InMemoryFormat.BINARY, KEY_COUNT, false, false, true},
        });
    }

    private boolean triggerForceStart = false;

    private ICache<Integer, Object> cache;

    @AfterClass
    public static void tearDownClass() {
        assertNoMBeanLeftovers();
    }

    @After
    public void tearDown() {
        cache.unwrap(ICache.class).destroy();
    }

    @Test
    public void test() {
        newInstances(CLUSTER_SIZE);
        cache = createCache();

        for (int key = 0; key < KEY_COUNT; key++) {
            String value = randomString();
            cache.put(key, value);
        }

        triggerForceStart = true;
        restartInstances(CLUSTER_SIZE);

        cache = createCache();
        assertEquals(0, cache.size());

        for (int key = 0; key < KEY_COUNT; key++) {
            String value = randomString();
            cache.put(key, value);
        }
        assertEquals(KEY_COUNT, cache.size());

        triggerForceStart = false;
        restartInstances(CLUSTER_SIZE);

        cache = createCache();
        assertEquals(KEY_COUNT, cache.size());
    }

    @Override
    Config makeConfig() {
        Config config = super.makeConfig();
        if (triggerForceStart) {
            config.addListenerConfig(new ListenerConfig(new TriggerForceStart()));
        }
        return config;
    }

    private class TriggerForceStart extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private Node node;

        @Override
        public void afterExpectedMembersJoin(Collection<? extends Member> members) {
            node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.node = getNode(instance);
        }

    }
}
