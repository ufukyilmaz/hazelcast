package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.spi.hotrestart.backup.AbstractHotRestartBackupTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BackupExecutorTest extends AbstractHotRestartBackupTest {

    @Test
    public void testExecutor() {
        final BackupExecutor executor = new BackupExecutor();
        final CountDownLatch latch = new CountDownLatch(1);

        assertEquals(Long.MIN_VALUE, executor.getBackupTaskMaxChunkSeq());

        executor.run(new CustomBackupTask(latch));
        assertFalse(executor.isBackupTaskDone());
        assertEquals(100, executor.getBackupTaskMaxChunkSeq());

        latch.countDown();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(executor.isBackupTaskDone());
            }
        });
    }

    @Test
    public void testTaskInterrupt() {
        final BackupExecutor executor = new BackupExecutor();
        executor.run(new InterruptableBackupTask());
        assertFalse(executor.isBackupTaskDone());

        executor.interruptBackupTask(true);
        assertTrue(!executor.inProgress());
    }

    private static class CustomBackupTask extends BackupTask {

        private final CountDownLatch latch;

        private BackupTaskState state = BackupTaskState.NOT_STARTED;

        CustomBackupTask(CountDownLatch latch) {
            super(null, "store", new long[0], new long[0]);
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            state = BackupTaskState.SUCCESS;
        }

        @Override
        public BackupTaskState getBackupState() {
            return state;
        }

        @Override
        public long getMaxChunkSeq() {
            return 100;
        }
    }

    private static class InterruptableBackupTask extends BackupTask {

        InterruptableBackupTask() {
            super(null, "store", new long[0], new long[0]);
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
