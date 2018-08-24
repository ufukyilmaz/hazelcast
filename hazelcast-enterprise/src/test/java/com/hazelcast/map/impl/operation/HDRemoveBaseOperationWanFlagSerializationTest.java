package com.hazelcast.map.impl.operation;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class HDRemoveBaseOperationWanFlagSerializationTest extends RemoveBaseOperationWanFlagSerializationTest {

    @Test
    public void testRemoveOperation() throws IOException {
        HDBaseRemoveOperation original = new HDRemoveOperation(MAP_NAME, keyMock, disableWanReplication);
        HDBaseRemoveOperation deserialized = new HDRemoveOperation();

        testSerialization(original, deserialized);
    }

    @Test
    public void testDeleteOperation() throws IOException {
        HDBaseRemoveOperation original = new HDDeleteOperation(MAP_NAME, keyMock, disableWanReplication);
        HDBaseRemoveOperation deserialized = new HDDeleteOperation();

        testSerialization(original, deserialized);
    }

    private void testSerialization(HDBaseRemoveOperation originalOp, HDBaseRemoveOperation deserializedOp) throws IOException {
        serializeAndDeserialize(originalOp, deserializedOp);

        assertEquals(originalOp.disableWanReplicationEvent, deserializedOp.disableWanReplicationEvent);
    }
}
