package com.hazelcast.map.impl.querycache.subscriber.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkHasText;

/**
 * Operation factory for {@link MadePublishableOperation}.
 *
 * @see MadePublishableOperation
 */
public class MadePublishableOperationFactory implements OperationFactory {

    private String mapName;
    private String cacheName;

    public MadePublishableOperationFactory() {
    }

    public MadePublishableOperationFactory(String mapName, String cacheName) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheName, "cacheName");

        this.cacheName = cacheName;
        this.mapName = mapName;
    }

    @Override
    public Operation createOperation() {
        return new MadePublishableOperation(mapName, cacheName);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeUTF(cacheName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        cacheName = in.readUTF();
    }
}

