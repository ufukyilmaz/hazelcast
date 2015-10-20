

package com.hazelcast.client.security;

import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.PartitionService;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientInstanceSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void addPartitionLostListener() {
        PartitionService partitionService = client.getPartitionService();
        interceptor.setExpectation(InternalPartitionService.SERVICE_NAME, null, "addPartitionLostListener");
        partitionService.addPartitionLostListener(mock(PartitionLostListener.class));
    }

    @Test
    public void removePartitionLostListener() {
        PartitionService partitionService = client.getPartitionService();
        String registrationID = partitionService.addPartitionLostListener(mock(PartitionLostListener.class));
        interceptor.setExpectation(InternalPartitionService.SERVICE_NAME, null, "removePartitionLostListener", SKIP_COMPARISON_OBJECT);
        partitionService.removePartitionLostListener(registrationID);
    }

    @Test
    public void addDistributedObjectListener() {
        interceptor.setExpectation(ProxyServiceImpl.SERVICE_NAME, null, "addDistributedObjectListener");
        client.addDistributedObjectListener(mock(DistributedObjectListener.class));
    }

    @Test
    public void removeDistributedObjectListener() {
        String registrationID = client.addDistributedObjectListener(mock(DistributedObjectListener.class));
        interceptor.setExpectation(ProxyServiceImpl.SERVICE_NAME, null, "removeDistributedObjectListener", SKIP_COMPARISON_OBJECT);
        client.removeDistributedObjectListener(registrationID);
    }

    @Test
    public void getDistributedObjects() {
        interceptor.setExpectation(ProxyServiceImpl.SERVICE_NAME, null, "getDistributedObjects");
        client.getDistributedObjects();
    }

}
