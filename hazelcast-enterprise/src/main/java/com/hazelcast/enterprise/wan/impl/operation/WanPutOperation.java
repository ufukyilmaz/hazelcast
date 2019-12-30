package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.wan.WanEnterpriseCacheEvent;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.wan.WanEnterpriseMapEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.impl.InternalWanEvent;

import java.io.IOException;

/**
 * Publishes a provided {@link WanEvent}. This operation is used
 * to put forwarded WAN events to the corresponding event queue.
 */
public class WanPutOperation extends WanBackupAwareOperation implements IdentifiedDataSerializable, ServiceNamespaceAware {
    private ServiceNamespace objectNamespace;
    private InternalWanEvent event;

    public WanPutOperation() {
    }

    public WanPutOperation(String wanReplicationName, String targetName, InternalWanEvent event, int backupCount) {
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
        return new WanPutBackupOperation(wanReplicationName, wanPublisherId, event, getServiceNamespace());
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
        return WanDataSerializerHook.EWR_PUT_OPERATION;
    }

    @Override
    public ServiceNamespace getServiceNamespace() {
        if (objectNamespace != null) {
            return objectNamespace;
        }
        switch (event.getServiceName()) {
            case MapService.SERVICE_NAME:
                final WanEnterpriseMapEvent mapEvent = (WanEnterpriseMapEvent) event;
                objectNamespace = MapService.getObjectNamespace(mapEvent.getMapName());
                break;
            case CacheService.SERVICE_NAME:
                final WanEnterpriseCacheEvent cacheEvent = (WanEnterpriseCacheEvent) event;
                objectNamespace = CacheService.getObjectNamespace(cacheEvent.getCacheName());
                break;
            default:
                throw new IllegalStateException("Forwarding WAN event for unknown service: " + event.getServiceName());
        }
        return objectNamespace;
    }
}
