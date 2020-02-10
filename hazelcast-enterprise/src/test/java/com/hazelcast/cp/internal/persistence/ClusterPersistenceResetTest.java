package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getAddresses;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterPersistenceResetTest extends PersistenceTestSupport {

    @Test
    public void when_cpSubsystemReset_then_startsEmpty() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        RaftGroupId group1 = invocationManager.createRaftGroup("group1").join();
        RaftGroupId group2 = invocationManager.createRaftGroup("group2").join();
        RaftGroupId group3 = invocationManager.createRaftGroup("group3").join();

        instances[0].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();
        long seed = getMetadataGroupId(instances[0]).getSeed();
        waitUntilCPDiscoveryCompleted(instances);

        factory.terminateAll();

        instances = restartInstances(addresses, config);
        RaftService raftService = getRaftService(instances[0]);

        Collection<CPGroupId> groupIds = raftService.getCPGroupIds().get();
        assertThat(groupIds, hasSize(1));

        assertNull(raftService.getCPGroup(group1.getName()).get());
        assertNull(raftService.getCPGroup(group2.getName()).get());
        assertNull(raftService.getCPGroup(group3.getName()).get());

        assertEquals(seed, getMetadataGroupId(instances[0]).getSeed());
    }

    @Test
    public void when_cpSubsystemIsResetWhileCPMemberIsDown_then_cpMemberCannotRestart() throws Exception {
        Config config = createConfig(3, 3);
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "30000");

        HazelcastInstance[] instances = factory.newInstances(config, 4);
        assertClusterSizeEventually(4, instances);

        for (HazelcastInstance instance : instances) {
            instance.getCPSubsystem().getCPSubsystemManagementService().awaitUntilDiscoveryCompleted(120, SECONDS);
        }

        Address restartingAddress = getAddress(instances[0]);
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(3, instances[1]);
        instances[1].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();

        for (HazelcastInstance instance : Arrays.asList(instances[1], instances[2], instances[3])) {
            instance.getCPSubsystem().getCPSubsystemManagementService().awaitUntilDiscoveryCompleted(120, SECONDS);
        }

        try {
            restartInstance(restartingAddress, config);
            fail();
        } catch (IllegalStateException ignored) {
        }
    }
}
