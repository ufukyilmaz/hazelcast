package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.wan.CacheReplicationObject;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;

/**
 * Publishes a provided {@link WanReplicationEvent}. This operation is used
 * to put forwarded WAN events to the corresponding event queue.
 */
// NOTE :
// Because 3.8- members first serialized the event using the serialization service
// and then wrote the Data to the output stream in writeInternal, we must do the same
// but we also need the deserialized event both on the sender and receiver because
// we can determine the object namespace from it. This is why we have the additional
// complexity
public class EWRPutOperation extends EWRBackupAwareOperation implements IdentifiedDataSerializable, ServiceNamespaceAware {
    private ServiceNamespace objectNamespace;
    private WanReplicationEvent deserializedEvent;
    private Data serializedEvent;

    public EWRPutOperation() {
    }

    public EWRPutOperation(String wanReplicationName, String targetName, WanReplicationEvent event, int backupCount) {
        super(wanReplicationName, targetName, backupCount);
        this.deserializedEvent = event;
    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService wanReplicationService = getEWRService();
        WanReplicationEndpoint endpoint = wanReplicationService.getEndpointOrFail(wanReplicationName, wanPublisherId);
        WanReplicationEvent event = getEvent();
        endpoint.publishReplicationEvent(event.getServiceName(), event.getEventObject());
        response = true;
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new EWRPutBackupOperation(wanReplicationName, wanPublisherId, getSerializedEvent(), getServiceNamespace());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        // the 3.8 code writes it exactly in this way - it serialises the event
        // and writes the Data.
        // This cannot simply be replaced with out.writeObject() because
        // that method omits the partition hash when serializing the object.
        // Because of this we would need to provide backwards compatibility.
        // The partition hash is not used on the operation receiver so we might
        // optimise this to use out.writeObject in the future
        out.writeData(getSerializedEvent());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        // see comment in writeInternal
        serializedEvent = in.readData();
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.EWR_PUT_OPERATION;
    }

    @Override
    public ServiceNamespace getServiceNamespace() {
        if (objectNamespace != null) {
            return objectNamespace;
        }
        final WanReplicationEvent event = getEvent();
        final String serviceName = event.getServiceName();

        if (MapService.SERVICE_NAME.equals(serviceName)) {
            final EnterpriseMapReplicationObject mapEvent = (EnterpriseMapReplicationObject) event.getEventObject();
            objectNamespace = MapService.getObjectNamespace(mapEvent.getMapName());
        } else if (CacheService.SERVICE_NAME.equals(serviceName)) {
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
