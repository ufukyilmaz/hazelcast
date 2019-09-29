package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.hotrestart.impl.di.DiContainer;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static com.hazelcast.internal.hotrestart.impl.gc.BackupTask.FAILURE_FILE;
import static com.hazelcast.internal.hotrestart.impl.gc.BackupTask.IN_PROGRESS_FILE;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.TOMB_BASEDIR;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.VAL_BASEDIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class})
public class BackupTaskTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private File destDir;
    private File sourceDir;

    @Before
    public void setup() throws IOException {
        destDir = tempDir.newFolder();
        sourceDir = tempDir.newFolder();
    }

    @Test
    public void testGetMaxChunkSeq() throws IOException {
        final BackupTask task = backupTask(mock(GcHelper.class), mock(ChunkManager.class));
        assertTrue(task.getMaxChunkSeq() == 1);
    }

    @Test
    public void testFailWhenInProgressExists() throws IOException {
        final BackupTask task = backupTask(mock(GcHelper.class), mock(ChunkManager.class));

        final File inProgress = new File(destDir, IN_PROGRESS_FILE);
        assertFalse(inProgress.exists());
        assertTrue(inProgress.createNewFile());
        assertEquals(BackupTaskState.NOT_STARTED, task.getBackupState());
        task.run();

        assertFailedTask(task);
    }

    @Test
    public void testFailWhenChunkFileDoesNotExist() throws IOException {
        final GcHelper helper = mock(GcHelper.class);
        when(helper.chunkFile(VAL_BASEDIR, 0, Chunk.FNAME_SUFFIX, false)).thenReturn(new File("nonExistant"));
        final BackupTask task = backupTask(helper, mock(ChunkManager.class));

        assertEquals(BackupTaskState.NOT_STARTED, task.getBackupState());
        task.run();
        assertFailedTask(task);
    }

    @Test
    public void testChunkFilesAreMovedWhenPendingDeletion() throws IOException {
        final File bucketDir = new File(new File(sourceDir, "base"), "bucket");
        assertFalse(bucketDir.exists());
        assertTrue(bucketDir.mkdirs());
        final File valChunk = new File(bucketDir, "valChunk");
        final File tombChunk = new File(bucketDir, "tombChunk");
        assertTrue(valChunk.createNewFile());
        assertTrue(tombChunk.createNewFile());

        final GcHelper helper = mock(GcHelper.class);
        when(helper.chunkFile(VAL_BASEDIR, 0, Chunk.FNAME_SUFFIX, false)).thenReturn(valChunk);
        when(helper.chunkFile(TOMB_BASEDIR, 1, Chunk.FNAME_SUFFIX, false)).thenReturn(tombChunk);

        final ChunkManager manager = mock(ChunkManager.class);
        when(manager.isChunkPendingDeletion(0, true)).thenReturn(true);

        final BackupTask task = backupTask(helper, manager);
        assertEquals(BackupTaskState.NOT_STARTED, task.getBackupState());
        task.run();
        assertEquals(BackupTaskState.SUCCESS, task.getBackupState());
        assertFalse(valChunk.exists());
        assertTrue(tombChunk.exists());
    }

    private void assertFailedTask(BackupTask task) throws IOException {
        final File failureFile = new File(destDir, FAILURE_FILE);
        assertTrue(failureFile.exists());
        assertEquals(BackupTaskState.FAILURE, task.getBackupState());

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(failureFile));
            assertTrue(reader.readLine().contains("Exception"));
        } finally {
            IOUtil.closeResource(reader);
        }
    }

    private BackupTask backupTask(GcHelper helper, ChunkManager manager) {
        return new DiContainer()
                .dep(GcLogger.class, mock(GcLogger.class))
                .dep(GcHelper.class, helper)
                .dep(ChunkManager.class, manager)
                .wire(new BackupTask(destDir, "store", new long[]{0}, new long[]{1}));
    }
}
