package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanElementCounterTest {

    @Test
    public void incrementCountersPrimary() {
        WanElementCounter wanCounter = givenWanCounter(1, 1);

        wanCounter.incrementCounters(false);

        assertEquals(2, wanCounter.getPrimaryElementCount());
        assertEquals(1, wanCounter.getBackupElementCount());
    }

    @Test
    public void incrementCountersBackup() {
        WanElementCounter wanCounter = givenWanCounter(1, 1);

        wanCounter.incrementCounters(true);

        assertEquals(1, wanCounter.getPrimaryElementCount());
        assertEquals(2, wanCounter.getBackupElementCount());
    }

    @Test
    public void decrementPrimaryElementCounter() {
        WanElementCounter wanCounter = givenWanCounter(1, 1);

        wanCounter.decrementPrimaryElementCounter();

        assertEquals(0, wanCounter.getPrimaryElementCount());
        assertEquals(1, wanCounter.getBackupElementCount());
    }

    @Test
    public void decrementBackupElementCounter() {
        WanElementCounter wanCounter = givenWanCounter(1, 1);

        wanCounter.decrementBackupElementCounter();

        assertEquals(1, wanCounter.getPrimaryElementCount());
        assertEquals(0, wanCounter.getBackupElementCount());
    }

    @Test
    public void decrementPrimaryElementCounterByDelta() {
        WanElementCounter wanCounter = givenWanCounter(2, 1);

        wanCounter.decrementPrimaryElementCounter(2);

        assertEquals(0, wanCounter.getPrimaryElementCount());
        assertEquals(1, wanCounter.getBackupElementCount());
    }

    @Test
    public void decrementBackupElementCounterByDelta() {
        WanElementCounter wanCounter = givenWanCounter(1, 2);

        wanCounter.decrementBackupElementCounter(2);

        assertEquals(1, wanCounter.getPrimaryElementCount());
        assertEquals(0, wanCounter.getBackupElementCount());
    }

    @Test
    public void moveFromBackupToPrimary() {
        WanElementCounter wanCounter = givenWanCounter(1, 10);

        wanCounter.moveFromBackupToPrimaryCounter(10);

        assertEquals(11, wanCounter.getPrimaryElementCount());
        assertEquals(0, wanCounter.getBackupElementCount());
    }

    @Test
    public void moveFromPrimaryToBackup() {
        WanElementCounter wanCounter = givenWanCounter(10, 1);

        wanCounter.moveFromPrimaryToBackupCounter(10);

        assertEquals(0, wanCounter.getPrimaryElementCount());
        assertEquals(11, wanCounter.getBackupElementCount());
    }

    @Test
    public void getPrimaryElementCount() {
        WanElementCounter wanCounter = givenWanCounter(1, 1);

        assertEquals(1, wanCounter.getPrimaryElementCount());
    }

    @Test
    public void getBackupElementCount() {
        WanElementCounter wanCounter = givenWanCounter(1, 1);

        assertEquals(1, wanCounter.getBackupElementCount());
    }

    private WanElementCounter givenWanCounter(int currentElementCounter, int currentBackupElementCounter) {
        WanElementCounter wanCounter = new WanElementCounter();
        wanCounter.setPrimaryElementCounter(currentElementCounter);
        wanCounter.setBackupElementCounter(currentBackupElementCounter);

        return wanCounter;
    }
}
