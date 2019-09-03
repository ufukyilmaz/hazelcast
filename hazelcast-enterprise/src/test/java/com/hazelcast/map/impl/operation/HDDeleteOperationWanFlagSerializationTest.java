package com.hazelcast.map.impl.operation;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class HDDeleteOperationWanFlagSerializationTest extends DeleteOperationWanFlagSerializationTest {

    @Test
    public void testDeleteOperation() throws IOException {
        DeleteOperation original = new DeleteOperation(MAP_NAME, keyMock, disableWanReplication);
        DeleteOperation deserialized = new DeleteOperation();

        testSerialization(original, deserialized);
    }

    private void testSerialization(DeleteOperation originalOp, DeleteOperation deserializedOp) throws IOException {
        serializeAndDeserialize(originalOp, deserializedOp);

        assertEquals(originalOp.disableWanReplicationEvent, deserializedOp.disableWanReplicationEvent);
    }
}
