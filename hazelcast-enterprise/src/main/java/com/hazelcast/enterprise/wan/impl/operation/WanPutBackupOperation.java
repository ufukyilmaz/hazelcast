package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.wan.WanEnterpriseCacheEvent;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.wan.WanEnterpriseMapEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.impl.InternalWanEvent;

import java.io.IOException;

/**
 * Publishes backup of a provided {@link WanEvent}. This operation is used
 * to put backups of forwarded WAN events to the corresponding backup event queue.
 */
public class WanPutBackupOperation extends WanBaseOperation
        implements BackupOperation, IdentifiedDataSerializable, ServiceNamespaceAware {
    private ServiceNamespace objectNamespace;
    private InternalWanEvent event;

    public WanPutBackupOperation() {
    }

    public WanPutBackupOperation(String wanReplicationName,
                                 String targetName,
                                 InternalWanEvent event,
                                 ServiceNamespace namespace) {
        super(wanReplicationName, targetName);
        this.event = event;
        this.objectNamespace = namespace;
    }

    @Override
    public void run() throws Exception {
        getEWRService().getPublisherOrFail(wanReplicationName, wanPublisherId)
                       .publishReplicationEventBackup(event);
        response = true;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.EWR_PUT_BACKUP_OPERATION;
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
