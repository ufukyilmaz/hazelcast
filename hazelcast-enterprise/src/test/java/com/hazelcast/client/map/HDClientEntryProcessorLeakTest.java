package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.instance.impl.TestUtil.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDClientEntryProcessorLeakTest extends HazelcastTestSupport {

    public static final String MAP_NAME = "EntryProcessorLeakTest";

    protected static TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    protected static HazelcastInstance client;
    protected static HazelcastInstance member;

    @Before
    public void setup() {
        Config config = getHDConfig();
        System.setProperty("hazelcast.partition.backup.sync.interval", "1000000");

        member = hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void stopHazelcastInstances() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testExecuteOnEntries_leak_issue3689() {
        MemoryStats memoryStats = getNode(member).hazelcastInstance.getMemoryStats();

        IMap<String, String> clientMap = client.getMap(MAP_NAME);
        clientMap.put("leaky", "value");

        // Run once to let all partitions initialize (eg. map.keySet() query will init all containers)
        clientMap.executeOnKeys(clientMap.keySet(), new ClientEntryProcessorTest.ValueUpdater("value"));

        // Compare for leak, before & after
        long totalBefore = memoryStats.getUsedNative();

        for (int i = 0; i < 100; i++) {
            clientMap.executeOnKeys(clientMap.keySet(), new ClientEntryProcessorTest.ValueUpdater("value"));
        }

        assertEquals(totalBefore, memoryStats.getUsedNative());
    }
}
