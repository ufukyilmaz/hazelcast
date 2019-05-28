package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseDataSerializableHeaderTest {

    @Test
    public void identified() {
        byte header = EnterpriseDataSerializableHeader.createHeader(true, false);

        assertTrue(EnterpriseDataSerializableHeader.isIdentifiedDataSerializable(header));
        assertFalse(EnterpriseDataSerializableHeader.isVersioned(header));
    }

    @Test
    public void versioned() {
        byte header = EnterpriseDataSerializableHeader.createHeader(false, true);

        assertFalse(EnterpriseDataSerializableHeader.isIdentifiedDataSerializable(header));
        assertTrue(EnterpriseDataSerializableHeader.isVersioned(header));
    }

    @Test
    public void all() {
        byte header = EnterpriseDataSerializableHeader.createHeader(true, true);

        assertTrue(EnterpriseDataSerializableHeader.isIdentifiedDataSerializable(header));
        assertTrue(EnterpriseDataSerializableHeader.isVersioned(header));
    }
}
