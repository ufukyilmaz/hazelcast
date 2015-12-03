package com.hazelcast.util.concurrent;

import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static com.hazelcast.util.concurrent.IdleStrategyTest.StrategyToTest.BACK_OFF;
import static com.hazelcast.util.concurrent.IdleStrategyTest.StrategyToTest.BUSY_SPIN;
import static com.hazelcast.util.concurrent.IdleStrategyTest.StrategyToTest.NO_OP;
import static java.util.Arrays.asList;
import static org.junit.Assert.fail;

@RunParallel
@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IdleStrategyTest {
    private static final int MAX_CALLS = 10;

    @Parameters(name = "manyToOne == {0}")
    public static Collection<Object[]> params() {
        return asList(new Object[][] { {NO_OP}, {BUSY_SPIN}, {BACK_OFF} });
    }

    @Parameter public StrategyToTest strategyToTest;

    private IdleStrategy idler;

    @Before public void setup() {
        this.idler = strategyToTest.create();
    }

    enum StrategyToTest {
        NO_OP     { IdleStrategy create() { return new NoOpIdleStrategy(); } },
        BUSY_SPIN { IdleStrategy create() { return new BusySpinIdleStrategy(); } },
        BACK_OFF  { IdleStrategy create() { return new BackoffIdleStrategy(1, 1, 1, 2); } }
        ;
        abstract IdleStrategy create();
    }

    @Test public void when_idle_thenReturnTrueEventually() {
        idler.idle(1);
        for (int i = 0; i < MAX_CALLS; i++) {
            if (idler.idle(0)) {
                return;
            }
        }
        fail();
    }
}
