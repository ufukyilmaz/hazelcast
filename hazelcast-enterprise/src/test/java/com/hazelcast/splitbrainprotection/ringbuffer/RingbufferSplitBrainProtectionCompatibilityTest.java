package com.hazelcast.splitbrainprotection.ringbuffer;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionCompatibilityTest;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class RingbufferSplitBrainProtectionCompatibilityTest extends AbstractSplitBrainProtectionCompatibilityTest {
    private int count;

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        Ringbuffer<String> ringbuffer = previousVersionMember.getRingbuffer(name);
        ringbuffer.add("1");
        assertEquals(1, ringbuffer.size());
        count = 1;
    }

    @Override
    protected void assertOperations_whileSplitBrainProtectionAbsent(HazelcastInstance member) {
        Ringbuffer<String> ringbuffer = member.getRingbuffer(name);
        ringbuffer.add("3");
    }

    @Override
    protected void assertOperations_whileSplitBrainProtectionPresent(HazelcastInstance member) {
        Ringbuffer<String> ringbuffer = member.getRingbuffer(name);
        ringbuffer.add(Integer.toString(++count));
        assertEquals(count, ringbuffer.size());
    }

    @Override
    protected Config getSplitBrainProtectedConfig() {
        return getConfigWithSplitBrainProtection()
                .addRingBufferConfig(new RingbufferConfig(name).setSplitBrainProtectionName("pq"));
    }
}
