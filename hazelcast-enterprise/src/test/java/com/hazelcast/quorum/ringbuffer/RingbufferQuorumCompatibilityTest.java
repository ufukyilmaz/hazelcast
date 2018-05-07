package com.hazelcast.quorum.ringbuffer;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class RingbufferQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {
    private int count;

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        Ringbuffer<String> ringbuffer = previousVersionMember.getRingbuffer(name);
        ringbuffer.add("1");
        assertEquals(1, ringbuffer.size());
        count = 1;
    }

    @Override
    protected void assertOperations_whileQuorumAbsent(HazelcastInstance member) {
        Ringbuffer<String> ringbuffer = member.getRingbuffer(name);
        ringbuffer.add("3");
    }

    @Override
    protected void assertOperations_whileQuorumPresent(HazelcastInstance member) {
        Ringbuffer<String> ringbuffer = member.getRingbuffer(name);
        ringbuffer.add(Integer.toString(++count));
        assertEquals(count, ringbuffer.size());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum()
                .addRingBufferConfig(new RingbufferConfig(name).setQuorumName("pq"));
    }
}
