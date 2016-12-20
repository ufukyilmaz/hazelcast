package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheForceStartTest extends AbstractCacheHotRestartTest {

    private static final int KEY_COUNT = 1000;

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, false}
        });
    }

    private int clusterSize = 3;
    private boolean triggerForceStart = false;

    @Test
    public void test() throws Exception {
        newInstances(clusterSize);
        ICache<Integer, Object> cache = createCache();

        for (int key = 0; key < KEY_COUNT; key++) {
            String value = randomString();
            cache.put(key, value);
        }

        triggerForceStart = true;
        restartInstances(clusterSize);

        cache = createCache();
        assertEquals(0, cache.size());

        for (int key = 0; key < KEY_COUNT; key++) {
            String value = randomString();
            cache.put(key, value);
        }
        assertEquals(KEY_COUNT, cache.size());

        triggerForceStart = false;
        restartInstances(clusterSize);

        cache = createCache();
        assertEquals(KEY_COUNT, cache.size());
    }

    private class TriggerForceStart extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private Node node;

        @Override
        public void afterAwaitUntilMembersJoin(Collection<? extends Member> members) {
            node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.node = getNode(instance);
        }

    }

    @Override
    Config makeConfig(Address address) {
        Config config = super.makeConfig(address);
        if (triggerForceStart) {
            config.addListenerConfig(new ListenerConfig(new TriggerForceStart()));
        }
        return config;
    }
}
