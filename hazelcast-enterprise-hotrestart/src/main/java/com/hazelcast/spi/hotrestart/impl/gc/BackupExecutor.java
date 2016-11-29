package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.nio.Disposable;
import com.hazelcast.util.EmptyStatement;

import static com.hazelcast.hotrestart.BackupTaskState.FAILURE;
import static com.hazelcast.hotrestart.BackupTaskState.SUCCESS;

/**
 * Executor in charge of running backup tasks. This one runs the backup task in a separate thread and exposes its
 * progress by the {@link #inProgress()} method.
 * Repeated calls of {@link #run(BackupTask)} will run just one backup task. The currently running task can be
 * interrupted by calling the {@link #dispose()} method.
 * An instance of this class can be reused by calling {@link #run(BackupTask)} with a new backup task.
 */
public class BackupExecutor implements Disposable {
    private volatile Thread currentBackupThread;
    private volatile BackupTask currentBackupTask;

    /**
     * Runs the backup task. If a backup task is already in progress returns without starting a new task. The backup task can
     * be interrupted by calling {@link #dispose()}.
     *
     * @param task the backup task to be run
     */
    public void run(final BackupTask task) {
        if (inProgress()) {
            return;
        }
        currentBackupTask = task;
        currentBackupThread = new Thread(task);
        currentBackupThread.start();
    }

    @Override
    public void dispose() {
        interruptBackupTask(true);
    }

    /**
     * Interrupts the backup task if one is currently running. The contents of the target backup directory will be left as-is
     *
     * @param waitForCompletion should this thread block until the backup thread terminates
     */
    public void interruptBackupTask(boolean waitForCompletion) {
        if (inProgress()) {
            currentBackupThread.interrupt();
            if (waitForCompletion) {
                try {
                    currentBackupThread.join();
                } catch (InterruptedException e) {
                    EmptyStatement.ignore(e);
                }
            }
        }
    }

    /** Returns if the backup task is in progress. */
    public boolean inProgress() {
        return currentBackupThread != null && currentBackupThread.isAlive();
    }

    /** Returns the current state of the backup task */
    public BackupTaskState getBackupTaskState() {
        return currentBackupTask != null ? currentBackupTask.getBackupState() : BackupTaskState.NOT_STARTED;
    }

    public long getBackupTaskMaxChunkSeq() {
        return currentBackupTask != null ? currentBackupTask.getMaxChunkSeq() : Long.MIN_VALUE;
    }

    public boolean isBackupTaskDone() {
        final BackupTaskState state = getBackupTaskState();
        return SUCCESS.equals(state) || FAILURE.equals(state);
    }
}
