package com.hazelcast.map.impl.client;

import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.subscriber.operation.SetReadCursorOperation;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

/**
 * Subscriber client-side request.
 *
 * @see com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperation
 */
public class SetReadCursorRequest extends PartitionClientRequest implements Portable, RetryableRequest {

    private String mapName;
    private String cacheName;
    private long sequence;
    private int partitionId;

    public SetReadCursorRequest() {
    }

    public SetReadCursorRequest(String mapName, String cacheName, long sequence, int partitionId) {
        this.mapName = mapName;
        this.cacheName = cacheName;
        this.sequence = sequence;
        this.partitionId = partitionId;
    }

    @Override
    protected Operation prepareOperation() {
        return new SetReadCursorOperation(mapName, cacheName, sequence, partitionId);
    }

    @Override
    protected int getPartition() {
        return partitionId;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EnterpriseMapPortableHook.SET_READ_CURSOR_REQUEST;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("mn", mapName);
        writer.writeUTF("cn", cacheName);
        writer.writeLong("sq", sequence);
        writer.writeInt("pi", partitionId);
    }

    public void read(PortableReader reader) throws IOException {
        mapName = reader.readUTF("mn");
        cacheName = reader.readUTF("cn");
        sequence = reader.readLong("sq");
        partitionId = reader.readInt("pi");
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return mapName;
    }
}
