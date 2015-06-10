package com.hazelcast.map.impl.client;

import com.hazelcast.client.impl.client.AllPartitionsClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperationFactory;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.security.Permission;
import java.util.Map;

/**
 * Subscriber client-side request.
 *
 * @see com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperation
 */
public class MadePublishableRequest extends AllPartitionsClientRequest implements Portable, RetryableRequest {

    private String mapName;
    private String cacheName;

    public MadePublishableRequest() {
    }

    public MadePublishableRequest(String mapName, String cacheName) {
        this.mapName = mapName;
        this.cacheName = cacheName;
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
        return EnterpriseMapPortableHook.MADE_PUBLISHABLE_REQUEST;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("mn", mapName);
        writer.writeUTF("cn", cacheName);
    }

    public void read(PortableReader reader) throws IOException {
        mapName = reader.readUTF("mn");
        cacheName = reader.readUTF("cn");
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new MadePublishableOperationFactory(mapName, cacheName);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return !map.containsValue(Boolean.FALSE);
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
