package com.hazelcast.map.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.event.BatchIMapEvent;
import com.hazelcast.map.impl.querycache.event.SingleIMapEvent;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.eventservice.impl.EmptyFilter;

import java.io.IOException;
import java.security.Permission;

/**
 * Adds listener adapter to {@link com.hazelcast.core.IMap IMap}
 */
public class MapAddListenerAdapterRequest extends CallableClientRequest {

    private static final EmptyFilter EMPTY_FILTER = new EmptyFilter();

    protected String name;

    public MapAddListenerAdapterRequest() {
        super();
    }

    public MapAddListenerAdapterRequest(String name) {
        this.name = name;
    }

    @Override
    public Object call() {
        ClientEndpoint endpoint = getEndpoint();
        ListenerAdapter adapter = createListenerAdapter(endpoint);
        return registerListener(endpoint, adapter);
    }

    private String registerListener(ClientEndpoint endpoint, ListenerAdapter adapter) {
        MapService mapService = (MapService) getService();
        EnterpriseMapServiceContext mapServiceContext
                = (EnterpriseMapServiceContext) mapService.getMapServiceContext();
        String registrationId = mapServiceContext.addListenerAdapter(adapter, EMPTY_FILTER, name);
        endpoint.setListenerRegistration(MapService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }

    private ListenerAdapter createListenerAdapter(final ClientEndpoint endpoint) {
        return new ListenerAdapter() {
            @Override
            public void onEvent(IMapEvent iMapEvent) {
                if (!endpoint.isAlive()) {
                    return;
                }
                Object eventData = getEventData(iMapEvent);
                endpoint.sendEvent(null, eventData, getCallId());
            }

            private Object getEventData(IMapEvent iMapEvent) {
                if (iMapEvent instanceof SingleIMapEvent) {
                    return ((SingleIMapEvent) iMapEvent).getEventData();
                }

                if (iMapEvent instanceof BatchIMapEvent) {
                    BatchIMapEvent batchIMapEvent = (BatchIMapEvent) iMapEvent;
                    return batchIMapEvent.getBatchEventData();
                }

                throw new IllegalArgumentException("Unexpected event type found = [" + iMapEvent + "]");
            }
        };
    }

    @Override
    public int getClassId() {
        return EnterpriseMapPortableHook.ADD_LISTENER_ADAPTER;
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapPortableHook.F_ID;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("name", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("name");
    }

}
