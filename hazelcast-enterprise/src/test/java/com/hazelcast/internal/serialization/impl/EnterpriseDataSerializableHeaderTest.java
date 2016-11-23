package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseDataSerializableHeaderTest {

    @Test
    public void identified() {
        byte header = EnterpriseDataSerializableHeader.createHeader(true, false, false);

        assertTrue(EnterpriseDataSerializableHeader.isIdentifiedDataSerializable(header));
        assertFalse(EnterpriseDataSerializableHeader.isVersioned(header));
        assertFalse(EnterpriseDataSerializableHeader.isCompressed(header));
    }

    @Test
    public void versioned() {
        byte header = EnterpriseDataSerializableHeader.createHeader(false, true, false);

        assertFalse(EnterpriseDataSerializableHeader.isIdentifiedDataSerializable(header));
        assertTrue(EnterpriseDataSerializableHeader.isVersioned(header));
        assertFalse(EnterpriseDataSerializableHeader.isCompressed(header));
    }

    @Test
    public void compressed() {
        byte header = EnterpriseDataSerializableHeader.createHeader(false, false, true);

        assertFalse(EnterpriseDataSerializableHeader.isIdentifiedDataSerializable(header));
        assertFalse(EnterpriseDataSerializableHeader.isVersioned(header));
        assertTrue(EnterpriseDataSerializableHeader.isCompressed(header));
    }

    @Test
    public void all() {
        byte header = EnterpriseDataSerializableHeader.createHeader(true, true, true);

        assertTrue(EnterpriseDataSerializableHeader.isIdentifiedDataSerializable(header));
        assertTrue(EnterpriseDataSerializableHeader.isVersioned(header));
        assertTrue(EnterpriseDataSerializableHeader.isCompressed(header));
    }

}