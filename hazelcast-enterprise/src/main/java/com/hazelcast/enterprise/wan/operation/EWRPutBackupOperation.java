package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.wan.CacheReplicationObject;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;

/**
 * Publishes backup of a provided {@link WanReplicationEvent}. This operation is used
 * to put backups of forwarded WAN events to the corresponding backup event queue.
 */
public class EWRPutBackupOperation extends EWRBaseOperation
        implements BackupOperation, IdentifiedDataSerializable, ServiceNamespaceAware {
    private ServiceNamespace objectNamespace;
    private WanReplicationEvent deserializedEvent;
    private Data serializedEvent;

    public EWRPutBackupOperation() {
    }

    public EWRPutBackupOperation(String wanReplicationName, String targetName, Data event, ServiceNamespace namespace) {
        super(wanReplicationName, targetName);
        this.serializedEvent = event;
        this.objectNamespace = namespace;
    }

    @Override
    public void run() throws Exception {
        WanReplicationEndpoint endpoint = getEWRService().getEndpointOrFail(wanReplicationName, wanPublisherId);
        endpoint.putBackup(getEvent());
        response = true;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.EWR_PUT_BACKUP_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        // see comment in EWRPutOperation.writeInternal
        out.writeData(getSerializedEvent());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        // see comment in EWRPutOperation.writeInternal
        serializedEvent = in.readData();
    }

    @Override
    public ServiceNamespace getServiceNamespace() {
        if (objectNamespace != null) {
            return objectNamespace;
        }
        final WanReplicationEvent event = getEvent();
        final String serviceName = event.getServiceName();
        final Object service = getNodeEngine().getService(serviceName);

        if (service instanceof MapService) {
            final EnterpriseMapReplicationObject mapEvent = (EnterpriseMapReplicationObject) event.getEventObject();
            objectNamespace = MapService.getObjectNamespace(mapEvent.getMapName());
        } else if (service instanceof CacheService) {
            final CacheReplicationObject cacheEvent = (CacheReplicationObject) event.getEventObject();
            objectNamespace = CacheService.getObjectNamespace(cacheEvent.getCacheName());
        } else {
            getLogger().warning("Forwarding WAN event for unknown service: " + serviceName);
        }
        return objectNamespace;
    }

    /** Returns the serialised format of the {@link WanReplicationEvent} for this operation */
    private Data getSerializedEvent() {
        if (serializedEvent == null) {
            serializedEvent = getNodeEngine().toData(deserializedEvent);
        }
        return serializedEvent;
    }

    /** Returns the deserialised format of the {@link WanReplicationEvent} for this operation */
    private WanReplicationEvent getEvent() {
        if (deserializedEvent == null) {
            deserializedEvent = getNodeEngine().toObject(serializedEvent);
        }
        return deserializedEvent;
    }
}
