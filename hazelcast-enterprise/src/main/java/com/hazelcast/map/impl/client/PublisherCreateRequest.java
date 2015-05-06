package com.hazelcast.map.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.InvocationClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.subscriber.operation.PublisherCreateOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.QueryResultSet;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Responsible for creating publisher part functionality of a {@link com.hazelcast.map.QueryCache QueryCache} system.
 */
public class PublisherCreateRequest extends InvocationClientRequest implements RetryableRequest {

    private AccumulatorInfo info;

    public PublisherCreateRequest() {
    }

    public PublisherCreateRequest(AccumulatorInfo info) {
        this.info = info;
    }

    @Override
    protected void invoke() {
        ClusterService clusterService = getClientEngine().getClusterService();
        Collection<MemberImpl> members = clusterService.getMemberList();
        List<Future> futures = new ArrayList<Future>(members.size());
        createInvocations(members, futures);
        List<QueryResultSet> results = getQueryResultSets(futures);
        getEndpoint().sendResponse(results, getCallId());
    }

    private List<QueryResultSet> getQueryResultSets(List<Future> futures) {
        List<QueryResultSet> results = new ArrayList<QueryResultSet>(futures.size());
        for (Future future : futures) {
            Object result = null;
            try {
                result = future.get();
            } catch (Throwable t) {
                ExceptionUtil.rethrow(t);
            }
            if (result == null) {
                continue;
            }
            QueryResultSet queryResultSet = (QueryResultSet) result;
            results.add(queryResultSet);
        }
        return results;
    }

    private void createInvocations(Collection<MemberImpl> members, List<Future> futures) {
        final ClientEndpoint endpoint = getEndpoint();
        for (MemberImpl member : members) {
            PublisherCreateOperation operation = new PublisherCreateOperation(info);
            operation.setCallerUuid(endpoint.getUuid());
            Address address = member.getAddress();
            InvocationBuilder invocationBuilder = createInvocationBuilder(SERVICE_NAME, operation, address);
            Future future = invocationBuilder.invoke();
            futures.add(future);
        }
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return EnterpriseMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EnterpriseMapPortableHook.CREATE_PUBLISHER_REQUEST;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        info.writePortable(writer);

    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        AccumulatorInfo accumulatorInfo = new AccumulatorInfo();
        accumulatorInfo.readPortable(reader);
        info = accumulatorInfo;
    }

    @Override
    public String toString() {
        return "PublisherCreateRequest{"
                + "info=" + info
                + '}';
    }
}

