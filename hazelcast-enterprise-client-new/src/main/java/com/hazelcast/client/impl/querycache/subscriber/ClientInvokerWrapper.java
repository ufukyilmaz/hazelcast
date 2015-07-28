package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Invocation functionality for client-side {@link QueryCacheContext}.
 *
 * @see InvokerWrapper
 */
public class ClientInvokerWrapper implements InvokerWrapper {

    private final QueryCacheContext context;
    private final ClientContext clientContext;

    public ClientInvokerWrapper(QueryCacheContext context, ClientContext clientContext) {
        this.context = context;
        this.clientContext = clientContext;
    }

    @Override
    public Future invokeOnPartitionOwner(Object request, int partitionId) {
        checkNotNull(request, "request cannot be null");
        checkNotNegative(partitionId, "partitionId");

        ClientRequest clientRequest = (ClientRequest) request;
        ClientInvocation clientInvocation = new ClientInvocation(getClient(), clientRequest, partitionId);
        ClientInvocationFuture future = clientInvocation.invoke();
        //todo: this needs to get fixed; I put a value in here so that it at least compiles.
        final ClientMessageDecoder clientMessageDecoder = null;
        return new ClientDelegatingFuture(future, clientContext.getSerializationService(), clientMessageDecoder);
    }

    @Override
    public Object invokeOnAllPartitions(Object request) {
        try {
            ClientMessage clientRequest = (ClientMessage) request;
            final Future future = new ClientInvocation(getClient(), clientRequest).invoke();
            Object result = future.get();
            return context.toObject(result);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Future invokeOnTarget(Object request, Address address) {
        checkNotNull(request, "request cannot be null");
        checkNotNull(address, "address cannot be null");

        ClientRequest clientRequest = (ClientRequest) request;
        ClientInvocation invocation = new ClientInvocation(getClient(), clientRequest, address);
        return invocation.invoke();
    }

    @Override
    public Object invoke(Object request) {
        checkNotNull(request, "request cannot be null");

        ClientInvocation invocation = new ClientInvocation(getClient(), (ClientMessage) request);
        ClientInvocationFuture future = invocation.invoke();
        try {
            Object result = future.get();
            return context.toObject(result);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public void executeOperation(Operation op) {
        throw new UnsupportedOperationException();
    }

    protected final HazelcastClientInstanceImpl getClient() {
        return (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
    }
}
