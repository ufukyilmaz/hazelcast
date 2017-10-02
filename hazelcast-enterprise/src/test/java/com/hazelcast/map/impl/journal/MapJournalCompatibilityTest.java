package com.hazelcast.map.impl.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({CompatibilityTest.class})
public class MapJournalCompatibilityTest extends HazelcastTestSupport {
    private CompatibilityTestHazelcastInstanceFactory factory;
    private String[] versions;

    @Before
    public void init() {
        this.versions = new String[]{"3.8", CURRENT_VERSION};
        this.factory = new CompatibilityTestHazelcastInstanceFactory(versions);
    }

    /**
     * The test asserts that a map put will complete without throwing
     * an exception when the cluster version in less than 3.9 but there
     * is a 3.9 member with the event journal enabled.
     * Previously the put operation failed with an exception saying that
     * the journal is not allowed when the cluster is running with a
     * version lower than 3.9. This is not a good user experience as
     * the user effectively cannot use the map until he first disables
     * the event journal. With the change, we simply act as if the journal
     * is disabled until the cluster version becomes 3.9.
     */
    @Test
    public void journalIsDisabledWhenClusterIsNot3_9() {
        final String mapName = "mappy";
        final Config config = getHDConfig()
                .addEventJournalConfig(new EventJournalConfig().setMapName(mapName).setEnabled(true));

        final HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance oldInstance = instances[0];
        final HazelcastInstance latestInstance = instances[1];
        assertClusterSizeEventually(versions.length, oldInstance);

        final IMap<Object, Object> map = latestInstance.getMap(mapName);
        final String key = generateKeyOwnedBy(latestInstance);
        map.put(key, "dummy");

        // assert that no journals (ringbuffers) were created on the current version
        final Node latestNode = getNode(latestInstance);
        final RingbufferService service = latestNode.nodeEngine.getService(RingbufferService.SERVICE_NAME);
        assertTrue(service.getContainers().isEmpty());

        oldInstance.shutdown();

        assertEquals(Versions.V3_8, latestInstance.getCluster().getClusterVersion());
        latestInstance.getCluster().changeClusterVersion(Versions.V3_9);
        assertEquals(Versions.V3_9, latestInstance.getCluster().getClusterVersion());

        map.put(key, "dummy");
        final int keyPartitionId = latestInstance.getPartitionService().getPartition(key).getPartitionId();

        assertFalse(service.getContainers().isEmpty());
        final RingbufferContainer<Object> journal = service.getContainerOrNull(keyPartitionId,
                MapService.getObjectNamespace(mapName));
        assertNotNull(journal);
        assertEquals(1, journal.size());
    }

    @After
    public void cleanup() {
        factory.shutdownAll();
    }
}
