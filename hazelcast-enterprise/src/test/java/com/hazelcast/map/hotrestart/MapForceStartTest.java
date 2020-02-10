package com.hazelcast.map.hotrestart;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.test.Accessors.getNode;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapForceStartTest extends AbstractMapHotRestartTest {

    private static final int CLUSTER_SIZE = 3;

    @Parameters(name = "memoryFormat:{0} fsync:{2} encrypted:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, KEY_COUNT, false, false, false},
                {InMemoryFormat.BINARY, KEY_COUNT, false, false, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, false, false, true},
        });
    }

    private boolean triggerForceStart = false;

    @Test
    public void test() {
        newInstances(CLUSTER_SIZE);
        IMap<Integer, Object> map = createMap();

        for (int key = 0; key < KEY_COUNT; key++) {
            String value = randomString();
            map.put(key, value);
        }

        triggerForceStart = true;
        restartInstances(CLUSTER_SIZE);

        map = createMap();
        assertEquals(0, map.size());

        for (int key = 0; key < KEY_COUNT; key++) {
            String value = randomString();
            map.put(key, value);
        }
        assertEquals(KEY_COUNT, map.size());

        triggerForceStart = false;
        restartInstances(CLUSTER_SIZE);

        map = createMap();
        assertEquals(KEY_COUNT, map.size());
    }

    @Override
    Config makeConfig(int backupCount) {
        Config config = super.makeConfig(backupCount);
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

    private static class TriggerForceStart extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

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
