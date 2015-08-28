

package com.hazelcast.client.security;

import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.PartitionService;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class ClientInstanceSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void addPartitionLostListener() {
        PartitionService partitionService = client.getPartitionService();
        partitionService.addPartitionLostListener(new PartitionLostListener() {
            @Override
            public void partitionLost(PartitionLostEvent event) {

            }
        });
        interceptor.assertMethod(InternalPartitionService.SERVICE_NAME, null, "addPartitionLostListener");
    }

    @Test
    public void removePartitionLostListener() {
        PartitionService partitionService = client.getPartitionService();
        String registrationID = partitionService.addPartitionLostListener(new PartitionLostListener() {
            @Override
            public void partitionLost(PartitionLostEvent event) {

            }
        });
        partitionService.removePartitionLostListener(registrationID);
        interceptor.assertMethod(InternalPartitionService.SERVICE_NAME, null, "removePartitionLostListener", registrationID);
    }

    @Test
    public void addDistributedObjectListener() {
        client.addDistributedObjectListener(new DistributedObjectListener() {
            @Override
            public void distributedObjectCreated(DistributedObjectEvent event) {

            }

            @Override
            public void distributedObjectDestroyed(DistributedObjectEvent event) {

            }
        });
        interceptor.assertMethod(ProxyServiceImpl.SERVICE_NAME, null, "addDistributedObjectListener");
    }

    @Test
    public void removeDistributedObjectListener() {
        String registrationID = client.addDistributedObjectListener(new DistributedObjectListener() {
            @Override
            public void distributedObjectCreated(DistributedObjectEvent event) {

            }

            @Override
            public void distributedObjectDestroyed(DistributedObjectEvent event) {

            }
        });
        client.removeDistributedObjectListener(registrationID);
        interceptor.assertMethod(ProxyServiceImpl.SERVICE_NAME, null, "removeDistributedObjectListener", registrationID);
    }

    @Test
    public void getDistributedObjects() {
        client.getDistributedObjects();
        interceptor.assertMethod(ProxyServiceImpl.SERVICE_NAME, null, "getDistributedObjects");
    }

}
