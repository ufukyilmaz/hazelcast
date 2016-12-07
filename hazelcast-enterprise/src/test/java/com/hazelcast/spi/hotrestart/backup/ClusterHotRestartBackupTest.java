package com.hazelcast.spi.hotrestart.backup;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterHotRestartBackupTest extends AbstractHotRestartBackupTest {
    @Test
    public void testClusterHotRestartBackup() throws IOException {
        final int clusterSize = 3;
        resetFixture(-1, clusterSize);

        final HashMap<Integer, String> expectedMap = new HashMap<Integer, String>();
        fillMap(expectedMap);

        final Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();

        final int backupSeq = 0;
        runClusterBackupOnInstance(backupSeq, instances.iterator().next());

        // wait until backups finish
        waitForBackupToFinish(instances);

        for (HazelcastInstance instance : instances) {
            final File nodeBackupDir = getNodeBackupDir(instance, backupSeq);
            assertTrue(nodeBackupDir.exists());
        }

        resetFixture(backupSeq, clusterSize);
        assertEquals(expectedMap.size(), map.size());
        assertContainsAll(map, expectedMap);
    }


    private void runClusterBackupOnInstance(long seq, HazelcastInstance instance) {
        instance.getCluster().getHotRestartService().backup(seq);
    }
}
