package com.hazelcast.internal.hotrestart;

import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.hotrestart.BackupTaskStatus;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.hotrestart.BackupTaskState.FAILURE;
import static com.hazelcast.hotrestart.BackupTaskState.IN_PROGRESS;
import static com.hazelcast.hotrestart.BackupTaskState.NOT_STARTED;
import static com.hazelcast.hotrestart.BackupTaskState.NO_TASK;
import static com.hazelcast.hotrestart.BackupTaskState.SUCCESS;
import static com.hazelcast.internal.hotrestart.HotRestartIntegrationService.getBackupTaskStatus;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartIntegrationServiceTest {

    @Test
    public void getBackupTaskStatus_AllNoTaskIsNoTask() {
        assertOverallState(NO_TASK);
        assertOverallState(NO_TASK, NO_TASK);
        assertOverallState(NO_TASK, NO_TASK, NO_TASK);
        assertOverallState(NO_TASK, NO_TASK, NO_TASK, NO_TASK);
    }

    @Test
    public void getBackupTaskStatus_AllSuccessIsSuccess() {
        assertOverallState(SUCCESS, SUCCESS);
        assertOverallState(SUCCESS, SUCCESS, SUCCESS);
        assertOverallState(SUCCESS, SUCCESS, SUCCESS, SUCCESS);
    }

    @Test
    public void getBackupTaskStatus_AnyFailureWhenAllAreCompletedIsFailure() {
        assertOverallState(FAILURE, FAILURE);
        assertOverallState(FAILURE, SUCCESS, FAILURE);
        assertOverallState(FAILURE, FAILURE, SUCCESS);
        assertOverallState(FAILURE, SUCCESS, FAILURE, SUCCESS);
        assertOverallState(FAILURE, SUCCESS, SUCCESS, FAILURE);
        assertOverallState(FAILURE, FAILURE, SUCCESS, SUCCESS);
    }

    @Test
    public void getBackupTaskStatus_AnyInProgressOrNotStartedIsInProgress() {
        assertOverallState(IN_PROGRESS, IN_PROGRESS);
        assertOverallState(IN_PROGRESS, IN_PROGRESS, IN_PROGRESS);
        assertOverallState(IN_PROGRESS, SUCCESS, IN_PROGRESS);
        assertOverallState(IN_PROGRESS, IN_PROGRESS, SUCCESS);
        assertOverallState(IN_PROGRESS, IN_PROGRESS, SUCCESS, IN_PROGRESS);
        assertOverallState(IN_PROGRESS, SUCCESS, IN_PROGRESS, IN_PROGRESS);
        assertOverallState(IN_PROGRESS, IN_PROGRESS, IN_PROGRESS, SUCCESS);
        assertOverallState(IN_PROGRESS, SUCCESS, IN_PROGRESS, SUCCESS);
        assertOverallState(IN_PROGRESS, NOT_STARTED, NOT_STARTED);
        assertOverallState(IN_PROGRESS, SUCCESS, NOT_STARTED);
        assertOverallState(IN_PROGRESS, NOT_STARTED, SUCCESS);
        assertOverallState(IN_PROGRESS, NOT_STARTED, SUCCESS, NOT_STARTED);
        assertOverallState(IN_PROGRESS, SUCCESS, NOT_STARTED, NOT_STARTED);
        assertOverallState(IN_PROGRESS, NOT_STARTED, NOT_STARTED, SUCCESS);
        assertOverallState(IN_PROGRESS, SUCCESS, NOT_STARTED, SUCCESS);
        assertOverallState(IN_PROGRESS, IN_PROGRESS, FAILURE);
        assertOverallState(IN_PROGRESS, FAILURE, IN_PROGRESS);
        assertOverallState(IN_PROGRESS, SUCCESS, IN_PROGRESS, FAILURE);
        assertOverallState(IN_PROGRESS, NOT_STARTED, SUCCESS, FAILURE);
    }

    private static void assertOverallState(BackupTaskState expected, BackupTaskState... actual) {
        HotRestartStore[] stores = new HotRestartStore[actual.length];
        int expectedCompleted = 0;
        for (int i = 0; i < stores.length; i++) {
            HotRestartStore store = mock(HotRestartStore.class);
            BackupTaskState state = actual[i];
            when(store.getBackupTaskState()).thenReturn(state);
            stores[i] = store;
            if (state == SUCCESS || state == FAILURE) {
                expectedCompleted++;
            }
        }
        BackupTaskStatus status = getBackupTaskStatus(stores);
        assertEquals(expectedCompleted, status.getCompleted());
        assertEquals(actual.length, status.getTotal());
        assertEquals(Arrays.toString(actual) + " should be " + expected + "\n", expected, status.getState());
    }
}
