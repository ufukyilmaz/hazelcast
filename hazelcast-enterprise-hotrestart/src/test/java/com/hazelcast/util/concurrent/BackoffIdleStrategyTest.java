package com.hazelcast.util.concurrent;


import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BackoffIdleStrategyTest {

    @Test public void when_proposedShiftLessThanAllowed_then_shiftProposed() {
        final BackoffIdleStrategy strat = new BackoffIdleStrategy(0, 0, 1, 4);

        assertEquals(1, strat.parkTime(0));
        assertEquals(2, strat.parkTime(1));
    }

    @Test public void when_maxShiftedGreaterThanMaxParkTime_thenParkMax() {
        final BackoffIdleStrategy strat = new BackoffIdleStrategy(0, 0, 3, 4);

        assertEquals(3, strat.parkTime(0));
        assertEquals(4, strat.parkTime(1));
        assertEquals(4, strat.parkTime(2));
    }

    @Test public void when_maxShiftedLessThanMaxParkTime_thenParkMaxShifted() {
        final BackoffIdleStrategy strat = new BackoffIdleStrategy(0, 0, 2, 3);

        assertEquals(2, strat.parkTime(0));
        assertEquals(3, strat.parkTime(1));
        assertEquals(3, strat.parkTime(2));
    }
}
