package com.hazelcast.spi.hotrestart.backup;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.hotrestart.HotBackupService;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.util.EmptyStatement;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterHotRestartBackupTest extends AbstractHotRestartBackupTest {

    @Test
    public void testClusterHotRestartBackup() {
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

    @Test
    public void testClusterHotRestartBackupInterrupt() throws IOException {
        final int clusterSize = 3;
        resetFixture(-1, clusterSize);
        final HashMap<Integer, String> expectedMap = new HashMap<Integer, String>();
        fillMap(expectedMap);
        final Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        final int backupSeq = 0;
        final HazelcastInstance firstNode = instances.iterator().next();

        runClusterBackupOnInstance(backupSeq, firstNode);
        firstNode.getCluster().getHotRestartService().interruptBackupTask();
        waitForBackupToFinish(instances);
    }

    @Test
    public void testClusterHotRestartBackupCommitFailed() {
        final int clusterSize = 3;
        resetFixture(-1, clusterSize);

        final HashMap<Integer, String> expectedMap = new HashMap<Integer, String>();
        fillMap(expectedMap);

        final Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        final HazelcastInstance[] instancesArr = instances.toArray(new HazelcastInstance[instances.size()]);

        final int backupSeq = 0;
        runBackupOnNode(instancesArr[2], backupSeq);
        // wait until backups finish
        waitForBackupToFinish(instances);

        try {
            runClusterBackupOnInstance(backupSeq, instancesArr[0]);
            fail("Hot backup should have failed because it was launched with the same backup seq");
        } catch (HotRestartException expected) {
            EmptyStatement.ignore(expected);
        }
    }

    @Test
    public void testClusterHotRestartBackupRollback() {
        final int clusterSize = 3;
        resetFixture(-1, clusterSize);

        final HashMap<Integer, String> expectedMap = new HashMap<Integer, String>();
        fillMap(expectedMap);

        final Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        final HazelcastInstance[] instancesArr = instances.toArray(new HazelcastInstance[instances.size()]);

        final HotBackupService backupService = (HotBackupService) getNode(instancesArr[1])
                .getNodeExtension().getHotRestartService();
        backupService.prepareBackup(getAddress(instancesArr[2]), "dummyTx", Long.MAX_VALUE);

        try {
            runClusterBackupOnInstance(0, instancesArr[0]);
            fail("Hot backup should have failed because a different backup request was already in progress");
        } catch (TransactionException expected) {
            EmptyStatement.ignore(expected);
        }
    }

    private static void runClusterBackupOnInstance(long seq, HazelcastInstance instance) {
        instance.getCluster().getHotRestartService().backup(seq);
    }
}