/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl.protocol.task.enterprisemap;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapAddListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.event.BatchEventData;
import com.hazelcast.map.impl.querycache.event.BatchIMapEvent;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.SingleIMapEvent;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type id:
 * {@link com.hazelcast.client.impl.protocol.codec.EnterpriseMapMessageType#ENTERPRISEMAP_ADDLISTENER}
 */
public class MapAddListenerMessageTask
        extends AbstractCallableMessageTask<EnterpriseMapAddListenerCodec.RequestParameters>
        implements ListenerAdapter {

    public MapAddListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ClientEndpoint endpoint = getEndpoint();
        return registerListener(endpoint, this);
    }

    private String registerListener(ClientEndpoint endpoint, ListenerAdapter adapter) {
        MapService mapService = getService(MapService.SERVICE_NAME);
        EnterpriseMapServiceContext mapServiceContext
                = (EnterpriseMapServiceContext) mapService.getMapServiceContext();
        String registrationId;
        if (parameters.localOnly) {
            registrationId = mapServiceContext.addLocalListenerAdapter(adapter, parameters.listenerName);
        } else {
            registrationId = mapServiceContext.addListenerAdapter(adapter,
                    TrueEventFilter.INSTANCE, parameters.listenerName);
        }

        endpoint.addListenerDestroyAction(MapService.SERVICE_NAME, parameters.listenerName, registrationId);
        return registrationId;
    }

    @Override
    public void onEvent(IMapEvent iMapEvent) {
        if (!endpoint.isAlive()) {
            return;
        }
        ClientMessage eventData = getEventData(iMapEvent);
        sendClientMessage(eventData);
    }

    private ClientMessage getEventData(IMapEvent iMapEvent) {
        if (iMapEvent instanceof SingleIMapEvent) {
            QueryCacheEventData eventData = ((SingleIMapEvent) iMapEvent).getEventData();
            ClientMessage clientMessage = EnterpriseMapAddListenerCodec.encodeQueryCacheSingleEvent(eventData);
            int partitionId = eventData.getPartitionId();
            clientMessage.setPartitionId(partitionId);
            return clientMessage;
        }

        if (iMapEvent instanceof BatchIMapEvent) {
            BatchIMapEvent batchIMapEvent = (BatchIMapEvent) iMapEvent;
            BatchEventData batchEventData = batchIMapEvent.getBatchEventData();
            int partitionId = batchEventData.getPartitionId();
            ClientMessage clientMessage =
                    EnterpriseMapAddListenerCodec.encodeQueryCacheBatchEvent(batchEventData.getEvents(),
                            batchEventData.getSource(), partitionId);
            clientMessage.setPartitionId(partitionId);
            return clientMessage;
        }

        throw new IllegalArgumentException("Unexpected event type found = [" + iMapEvent + "]");
    }

    @Override
    protected EnterpriseMapAddListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return EnterpriseMapAddListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return EnterpriseMapAddListenerCodec.encodeResponse((String) response);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.listenerName;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
