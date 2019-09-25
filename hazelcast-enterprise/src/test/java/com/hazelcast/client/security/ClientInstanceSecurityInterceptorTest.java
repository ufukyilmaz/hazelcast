package com.hazelcast.client.security;

import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.mockito.Mockito.mock;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientInstanceSecurityInterceptorTest extends InterceptorTestSupport {

    @Test
    public void addPartitionLostListener() {
        PartitionService partitionService = client.getPartitionService();
        interceptor.setExpectation(IPartitionService.SERVICE_NAME, null, "addPartitionLostListener");
        partitionService.addPartitionLostListener(mock(PartitionLostListener.class));
    }

    @Test
    public void removePartitionLostListener() {
        PartitionService partitionService = client.getPartitionService();
        UUID registrationID = partitionService.addPartitionLostListener(mock(PartitionLostListener.class));
        interceptor.setExpectation(IPartitionService.SERVICE_NAME, null, "removePartitionLostListener", SKIP_COMPARISON_OBJECT);
        partitionService.removePartitionLostListener(registrationID);
    }

    @Test
    public void addDistributedObjectListener() {
        interceptor.setExpectation(ProxyServiceImpl.SERVICE_NAME, null, "addDistributedObjectListener");
        client.addDistributedObjectListener(mock(DistributedObjectListener.class));
    }

    @Test
    public void removeDistributedObjectListener() {
        UUID registrationID = client.addDistributedObjectListener(mock(DistributedObjectListener.class));
        interceptor.setExpectation(ProxyServiceImpl.SERVICE_NAME, null, "removeDistributedObjectListener", SKIP_COMPARISON_OBJECT);
        client.removeDistributedObjectListener(registrationID);
    }

    @Test
    public void getDistributedObjects() {
        interceptor.setExpectation(ProxyServiceImpl.SERVICE_NAME, null, "getDistributedObjects");
        client.getDistributedObjects();
    }

}
