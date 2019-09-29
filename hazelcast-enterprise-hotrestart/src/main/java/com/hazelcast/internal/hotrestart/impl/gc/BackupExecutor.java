package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.internal.nio.Disposable;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.lang.Thread.currentThread;

/**
 * Executor in charge of running backup tasks. This one runs the backup task in a separate thread and exposes its
 * progress by the {@link #inProgress()} method.
 * Repeated calls of {@link #run(BackupTask)} will run just one backup task. The currently running task can be
 * interrupted by calling the {@link #dispose()} method.
 * An instance of this class can be reused by calling {@link #run(BackupTask)} with a new backup task.
 */
public class BackupExecutor implements Disposable {
    private static final AtomicReferenceFieldUpdater<BackupExecutor, BackupTask> CURRENT_BACKUP_TASK_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(BackupExecutor.class, BackupTask.class, "currentBackupTask");
    private static final long[] EMPTY_LONG_ARRAY = new long[0];
    private volatile Thread currentBackupThread;
    private volatile BackupTask currentBackupTask;
    private final BackupTask taskReservation = new BackupTask(null, "reservationStore",
            EMPTY_LONG_ARRAY, EMPTY_LONG_ARRAY);

    /**
     * Runs the backup task. If a backup task is already in progress returns without starting a new task. The backup task can
     * be interrupted by calling {@link #dispose()}. This method should be run from a single thread.
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
                    currentThread().interrupt();
                }
            }
        }
    }

    /** Returns if the backup task is in progress. */
    public boolean inProgress() {
        return currentBackupThread != null && currentBackupThread.isAlive();
    }

    /**
     * If there is no currently running task, makes a reservation so that following invocations of {@link #getBackupTaskState()}
     * return {@link BackupTaskState#NOT_STARTED}. If invoked concurrently, only one invocation will return {@code true},
     * thus enabling prevention of concurrent backup task runs.
     *
     * @return if the preparation succeeded
     */
    public boolean prepareForNewTask() {
        final BackupTask currentTask = this.currentBackupTask;
        final BackupTaskState taskState = currentTask != null ? currentTask.getBackupState() : BackupTaskState.NO_TASK;
        return !taskState.inProgress() && CURRENT_BACKUP_TASK_UPDATER.compareAndSet(this, currentTask, taskReservation);
    }

    /** Returns the current state of the last backup task */
    public BackupTaskState getBackupTaskState() {
        return currentBackupTask != null ? currentBackupTask.getBackupState() : BackupTaskState.NO_TASK;
    }

    /** Returns the maximum chunk seq that the last submitted task will or has backed up */
    public long getBackupTaskMaxChunkSeq() {
        return currentBackupTask != null ? currentBackupTask.getMaxChunkSeq() : Long.MIN_VALUE;
    }

    /** Returns true if the backup task is done (successfully or with failure) or if there is no submitted backup task */
    public boolean isBackupTaskDone() {
        return !getBackupTaskState().inProgress();
    }
}
