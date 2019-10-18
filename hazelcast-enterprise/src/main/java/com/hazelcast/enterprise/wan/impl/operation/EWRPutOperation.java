package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.wan.CacheReplicationObject;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;

/**
 * Publishes a provided {@link WanReplicationEvent}. This operation is used
 * to put forwarded WAN events to the corresponding event queue.
 */
public class EWRPutOperation extends EWRBackupAwareOperation implements IdentifiedDataSerializable, ServiceNamespaceAware {
    private ServiceNamespace objectNamespace;
    private WanReplicationEvent event;

    public EWRPutOperation() {
    }

    public EWRPutOperation(String wanReplicationName, String targetName, WanReplicationEvent event, int backupCount) {
        super(wanReplicationName, targetName, backupCount);
        this.event = event;
    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService wanReplicationService = getEWRService();
        wanReplicationService.getPublisherOrFail(wanReplicationName, wanPublisherId)
                             .publishReplicationEvent(event);
        response = true;
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new EWRPutBackupOperation(wanReplicationName, wanPublisherId, event, getServiceNamespace());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(event);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        event = in.readObject();
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.EWR_PUT_OPERATION;
    }

    @Override
    public ServiceNamespace getServiceNamespace() {
        if (objectNamespace != null) {
            return objectNamespace;
        }
        final String serviceName = event.getServiceName();

        if (MapService.SERVICE_NAME.equals(serviceName)) {
            final EnterpriseMapReplicationObject mapEvent = (EnterpriseMapReplicationObject) event;
            objectNamespace = MapService.getObjectNamespace(mapEvent.getMapName());
        } else if (CacheService.SERVICE_NAME.equals(serviceName)) {
            final CacheReplicationObject cacheEvent = (CacheReplicationObject) event;
            objectNamespace = CacheService.getObjectNamespace(cacheEvent.getCacheName());
        } else {
            getLogger().warning("Forwarding WAN event for unknown service: " + serviceName);
        }
        return objectNamespace;
    }
}