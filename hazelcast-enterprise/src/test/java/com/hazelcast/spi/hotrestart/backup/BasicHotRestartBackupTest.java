package com.hazelcast.spi.hotrestart.backup;

import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BasicHotRestartBackupTest extends AbstractHotRestartBackupTest {

    @Test
    public void testSingleNodePut() {
        resetFixture(-1, 1);
        final Map<Integer, String> mapWithBackup = new HashMap<Integer, String>();
        final Map<Integer, String> mapWithoutBackup = new HashMap<Integer, String>();

        fillMap(mapWithBackup);
        final int backupSeq = 0;
        runBackupOnEachNode(backupSeq);

        fillMap(mapWithoutBackup);

        resetFixture(backupSeq, 1);
        assertEquals(mapWithBackup.size(), map.size());
        assertContainsAll(map, mapWithBackup);

        resetFixture(-1, 1);
        assertEquals(mapWithoutBackup.size(), map.size());
        assertContainsAll(map, mapWithoutBackup);
    }

    @Test(expected = HotRestartException.class)
    public void testFailBackupFolderAlreadyExists() {
        resetFixture(-1, 1);
        fillMap(null);
        final int backupSeq = 0;

        getNodeBackupDir(getFirstInstance(), backupSeq).mkdirs();
        runBackupOnEachNode(backupSeq);
    }

    @Test
    public void testFailBackupNotConfigured() {
        resetFixture(-1, 1, false);
        fillMap(null);
        final boolean[] backupResults = runBackupOnEachNode(0);
        for (boolean result : backupResults) {
            assertFalse(result);
        }
    }

    @Test
    public void testConsistentBackupsWhileMutation() {
        resetFixture(-1, 1);
        final int keyCount = 100000;
        for (int i = 0; i < keyCount; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < keyCount * 2; i++) {
            map.put(i % keyCount, i % keyCount);
            if (i > keyCount && i % (keyCount / 3) == 0) {
                runBackupOnEachNode(i);
            }
        }
        final Collection<HazelcastInstance> instances = getAllHazelcastInstances();
        waitForBackupToFinish(instances);

        final HazelcastInstance firstNode = instances.iterator().next();
        final HotRestartPersistenceConfig persistenceConfig = firstNode.getConfig().getHotRestartPersistenceConfig();

        for (String backupDir : persistenceConfig.getBackupDir().list()) {
            resetFixture(Long.valueOf(backupDir.replace("backup-", "")), 1);
            assertEquals(keyCount, map.size());
            for (int i = 0; i < keyCount; i++) {
                assertEquals(i, map.get(i));
            }
        }
    }

    private boolean[] runBackupOnEachNode(long seq) {
        final Collection<HazelcastInstance> instances = getAllHazelcastInstances();
        final boolean[] backupsRun = new boolean[instances.size()];
        int backupIdx = 0;
        for (HazelcastInstance instance : instances) {
            backupsRun[backupIdx++] = runBackupOnNode(instance, seq);
        }
        return backupsRun;
    }
}
