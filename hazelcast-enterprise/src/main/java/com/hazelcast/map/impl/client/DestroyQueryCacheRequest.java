package com.hazelcast.map.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.InvocationClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.subscriber.operation.DestroyQueryCacheOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.util.FutureUtil;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Client request for {@link DestroyQueryCacheOperation}
 *
 * @see DestroyQueryCacheOperation
 */
public class DestroyQueryCacheRequest extends InvocationClientRequest implements Portable, RetryableRequest {

    private String mapName;
    private String cacheName;

    public DestroyQueryCacheRequest() {
    }

    public DestroyQueryCacheRequest(String cacheName, String mapName) {
        this.cacheName = cacheName;
        this.mapName = mapName;
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
        return EnterpriseMapPortableHook.REMOVE_PUBLISHER_REQUEST;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    protected void invoke() {
        ClusterService clusterService = getClientEngine().getClusterService();
        Collection<MemberImpl> members = clusterService.getMemberList();
        List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>(members.size());
        createInvocations(members, futures);
        Collection<Boolean> results = FutureUtil.returnWithDeadline(futures, 1, TimeUnit.MINUTES);
        getEndpoint().sendResponse(results, getCallId());
    }

    private void createInvocations(Collection<MemberImpl> members, List<Future<Boolean>> futures) {
        final ClientEndpoint endpoint = getEndpoint();
        for (MemberImpl member : members) {
            DestroyQueryCacheOperation operation = new DestroyQueryCacheOperation(mapName, cacheName);
            operation.setCallerUuid(endpoint.getUuid());
            Address address = member.getAddress();
            InvocationBuilder invocationBuilder = createInvocationBuilder(SERVICE_NAME, operation, address);
            Future future = invocationBuilder.invoke();
            futures.add(future);
        }
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF(mapName, "mn");
        writer.writeUTF(cacheName, "cn");

    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        mapName = reader.readUTF("mn");
        cacheName = reader.readUTF("cn");
    }

    @Override
    public String toString() {
        return "RemovePublisherRequest{"
                + "cacheName='" + cacheName + '\''
                + ", mapName='" + mapName + '\''
                + '}';
    }
}
