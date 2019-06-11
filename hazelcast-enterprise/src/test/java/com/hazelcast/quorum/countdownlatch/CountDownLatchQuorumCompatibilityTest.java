package com.hazelcast.quorum.countdownlatch;

import com.hazelcast.config.Config;
import com.hazelcast.config.CountDownLatchConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class CountDownLatchQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    private int count;

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        ICountDownLatch latch = previousVersionMember.getCountDownLatch(name);
        assertTrue(latch.trySetCount(2));
        count = 2;
    }

    @Override
    protected void assertOperations_whileQuorumAbsent(HazelcastInstance member) {
        ICountDownLatch latch = member.getCountDownLatch(name);
        latch.countDown();
    }

    @Override
    protected void assertOperations_whileQuorumPresent(HazelcastInstance member) {
        ICountDownLatch latch = member.getCountDownLatch(name);
        latch.countDown();
        count--;
        assertEquals(count, latch.getCount());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum()
                .addCountDownLatchConfig(new CountDownLatchConfig(name).setQuorumName("pq"));
    }
}
