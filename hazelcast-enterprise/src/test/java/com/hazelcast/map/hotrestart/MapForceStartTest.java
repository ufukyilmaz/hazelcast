package com.hazelcast.map.hotrestart;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
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
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapForceStartTest extends AbstractMapHotRestartTest {

    private static final int KEY_COUNT = 1000;

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                // FIXME: https://github.com/hazelcast/hazelcast-enterprise/issues/1689
                //{InMemoryFormat.NATIVE, KEY_COUNT, false},
                {InMemoryFormat.BINARY, KEY_COUNT, false},
        });
    }

    private int clusterSize = 3;
    private boolean triggerForceStart = false;

    @Test
    public void test() throws Exception {
        newInstances(clusterSize);
        IMap<Integer, Object> map = createMap();

        for (int key = 0; key < KEY_COUNT; key++) {
            String value = randomString();
            map.put(key, value);
        }

        triggerForceStart = true;
        restartInstances(clusterSize);

        map = createMap();
        assertEquals(0, map.size());

        for (int key = 0; key < KEY_COUNT; key++) {
            String value = randomString();
            map.put(key, value);
        }
        assertEquals(KEY_COUNT, map.size());

        triggerForceStart = false;
        restartInstances(clusterSize);

        map = createMap();
        assertEquals(KEY_COUNT, map.size());
    }

    private static class TriggerForceStart extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private Node node;

        public void afterExpectedMembersJoin(Collection<? extends Member> members) {
            node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.node = getNode(instance);
        }

    }

    @Override
    Config makeConfig(Address address, int backupCount) {
        Config config = super.makeConfig(address, backupCount);
        if (triggerForceStart) {
            config.addListenerConfig(new ListenerConfig(new TriggerForceStart()));
        } else {
            Iterator<ListenerConfig> iterator = config.getListenerConfigs().iterator();
            while (iterator.hasNext()) {
                ListenerConfig cfg = iterator.next();
                if (cfg.getImplementation() != null && cfg.getImplementation().getClass() == TriggerForceStart.class) {
                    iterator.remove();
                }

            }
        }
        return config;
    }
}
