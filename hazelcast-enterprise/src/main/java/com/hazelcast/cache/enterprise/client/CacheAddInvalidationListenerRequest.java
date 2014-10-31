package com.hazelcast.cache.enterprise.client;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.security.Permission;

/**
 * @author mdogan 18/02/14
 */
public class CacheAddInvalidationListenerRequest
        extends CallableClientRequest
        implements RetryableRequest {

    private String name;

    public CacheAddInvalidationListenerRequest() {
    }

    public CacheAddInvalidationListenerRequest(String name) {
        this.name = name;
    }

    @Override
    public Object call() {
        ClientEndpoint endpoint = getEndpoint();
        EnterpriseCacheService cacheService = getService();
        CacheInvalidationListener listener = new CacheInvalidationListener(endpoint, getCallId());
        String registrationId = cacheService.addInvalidationListener(name, listener);
        endpoint.setListenerRegistration(EnterpriseCacheService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }

    public String getServiceName() {
        return EnterpriseCacheService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.ADD_INVALIDATION_LISTENER;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
