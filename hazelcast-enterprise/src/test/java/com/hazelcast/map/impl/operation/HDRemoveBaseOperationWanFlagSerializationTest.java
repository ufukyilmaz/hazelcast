package com.hazelcast.map.impl.operation;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class HDRemoveBaseOperationWanFlagSerializationTest extends RemoveBaseOperationWanFlagSerializationTest {

    @Test
    public void testRemoveOperation() throws IOException {
        BaseRemoveOperation original = new RemoveOperation(MAP_NAME, keyMock, disableWanReplication);
        BaseRemoveOperation deserialized = new RemoveOperation();

        testSerialization(original, deserialized);
    }

    @Test
    public void testDeleteOperation() throws IOException {
        BaseRemoveOperation original = new DeleteOperation(MAP_NAME, keyMock, disableWanReplication);
        BaseRemoveOperation deserialized = new DeleteOperation();

        testSerialization(original, deserialized);
    }

    private void testSerialization(BaseRemoveOperation originalOp, BaseRemoveOperation deserializedOp) throws IOException {
        serializeAndDeserialize(originalOp, deserializedOp);

        assertEquals(originalOp.disableWanReplicationEvent, deserializedOp.disableWanReplicationEvent);
    }
}
