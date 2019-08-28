package com.hazelcast.internal.memory.impl;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class UnsafeMallocTest extends AbstractMallocTest {

    @ClassRule
    public static final TestIgnoreRuleAccordingToUnsafeAvailability UNSAFE_AVAILABILITY_RULE
            = new TestIgnoreRuleAccordingToUnsafeAvailability();

    @Override
    LibMalloc getLibMalloc() {
        return new UnsafeMalloc();
    }

    @Test
    public void test_toString() {
        assertEquals("UnsafeMalloc", getLibMalloc().toString());
    }

}
