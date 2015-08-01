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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapDestroyCacheCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.querycache.subscriber.operation.DestroyQueryCacheOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.FutureUtil;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Client Protocol Task for handling messages with type id:
 * {@link com.hazelcast.client.impl.protocol.codec.EnterpriseMapMessageType#ENTERPRISEMAP_DESTROYCACHE}
 */
public class MapDestroyCacheMessageTask
        extends AbstractCallableMessageTask<EnterpriseMapDestroyCacheCodec.RequestParameters> {

    public MapDestroyCacheMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ClusterService clusterService = clientEngine.getClusterService();
        Collection<MemberImpl> members = clusterService.getMemberImpls();
        List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>(members.size());
        createInvocations(members, futures);
        Collection<Boolean> results = FutureUtil.returnWithDeadline(futures, 1, TimeUnit.MINUTES);
        return reduce(results);
    }

    private boolean reduce(Collection<Boolean> results) {
        return !results.contains(Boolean.FALSE);
    }

    private void createInvocations(Collection<MemberImpl> members, List<Future<Boolean>> futures) {
        InternalOperationService operationService = nodeEngine.getOperationService();
        for (MemberImpl member : members) {
            DestroyQueryCacheOperation operation =
                    new DestroyQueryCacheOperation(parameters.mapName, parameters.cacheName);
            operation.setCallerUuid(endpoint.getUuid());
            Address address = member.getAddress();
            InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, address);
            Future future = invocationBuilder.invoke();
            futures.add(future);
        }
    }

    @Override
    protected EnterpriseMapDestroyCacheCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return EnterpriseMapDestroyCacheCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return EnterpriseMapDestroyCacheCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
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
