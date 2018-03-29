package com.hazelcast.quorum.ringbuffer;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.quorum.QuorumCompatibilityTest;
import com.hazelcast.ringbuffer.Ringbuffer;

import static org.junit.Assert.assertEquals;

public class RingbufferQuorumCompatibilityTest extends QuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        Ringbuffer<String> ringbuffer = previousVersionMember.getRingbuffer(name);
        ringbuffer.add("1");
        assertEquals(1, ringbuffer.size());
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        Ringbuffer<String> ringbuffer = member.getRingbuffer(name);
        ringbuffer.add("2");
        assertEquals(2, ringbuffer.size());
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        Ringbuffer<String> ringbuffer = member.getRingbuffer(name);
        ringbuffer.add("3");
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        Ringbuffer<String> ringbuffer = member.getRingbuffer(name);
        ringbuffer.add("4");
        assertEquals(3, ringbuffer.size());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum()
                .addRingBufferConfig(new RingbufferConfig(name).setQuorumName("pq"));
    }
}
