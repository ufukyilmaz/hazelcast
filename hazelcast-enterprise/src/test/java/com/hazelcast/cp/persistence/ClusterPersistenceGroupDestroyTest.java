package com.hazelcast.cp.persistence;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.raftop.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;
import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterPersistenceGroupDestroyTest extends PersistenceTestSupport {

    @Test
    public void when_groupDestroyed_then_itsDirShouldBeDeleted() throws Exception {
        when_groupDestroyed_then_itsDirShouldBeDeleted(false, false);
    }

    @Test
    public void when_groupDestroyed_afterLeaderElection_then_itsDirShouldBeDeleted() throws Exception {
        when_groupDestroyed_then_itsDirShouldBeDeleted(false, true);
    }

    @Test
    public void when_groupForceDestroyed_then_itsDirShouldBeDeleted() throws Exception {
        when_groupDestroyed_then_itsDirShouldBeDeleted(true, false);
    }

    @Test
    public void when_groupForceDestroyed_afterLeaderElection_then_itsDirShouldBeDeleted() throws Exception {
        when_groupDestroyed_then_itsDirShouldBeDeleted(true, true);
    }

    private void when_groupDestroyed_then_itsDirShouldBeDeleted(boolean forceDestroy, boolean waitLeaderElection)
            throws Exception {
        assumeThat("This test does not involve restart", restartAddressPolicy, equalTo(AddressPolicy.REUSE_EXACT));

        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);

        for (int i = 0; i < 3; i++) {
            RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
            RaftGroupId group = invocationManager.createRaftGroup("group" + i).join();

            if (waitLeaderElection) {
                waitAllForLeaderElection(instances, group);
            }

            if (forceDestroy) {
                getRaftService(instances[0]).forceDestroyCPGroup(group.getName()).get();
            } else {
                destroyRaftGroup(instances[0], group);
            }

            assertTrueEventually(() -> {
                for (HazelcastInstance instance : instances) {
                    RaftService raftService = getRaftService(instance);
                    assertNull(raftService.getRaftNode(group));

                    CPPersistenceServiceImpl cpPersistenceService = getCpPersistenceService(instance);
                    File groupDir = cpPersistenceService.getGroupDir(group);
                    assertFalse(group + " directory '" + groupDir + "' should not exist!", groupDir.exists());
                }

                RaftService raftService = getRaftService(instances[0]);
                Collection<CPGroupId> groupIds = raftService.getCPGroupIds().get();
                assertThat("Groups: " + groupIds, groupIds, hasSize(1));
                assertThat(groupIds, not(hasItem(group)));
                assertNull(raftService.getCPGroup(group.getName()).get());
            });
        }
    }

    @Test
    public void when_groupDestroyed_then_itShouldNotBeRestored() throws Exception {
        when_groupDestroyed_then_itShouldNotBeRestored(false);
    }

    @Test
    public void when_groupForceDestroyed_then_itShouldNotBeRestored() throws Exception {
        when_groupDestroyed_then_itShouldNotBeRestored(true);
    }

    private void when_groupDestroyed_then_itShouldNotBeRestored(boolean forceDestroy) throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        for (int i = 0; i < 3; i++) {
            RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
            RaftGroupId group1 = invocationManager.createRaftGroup("group1").join();
            RaftGroupId group2 = invocationManager.createRaftGroup("group2").join();

            if (forceDestroy) {
                getRaftService(instances[0]).forceDestroyCPGroup(group2.getName()).get();
            } else {
                destroyRaftGroup(instances[0], group2);
            }

            factory.terminateAll();

            instances = restartInstances(addresses, config);

            HazelcastInstance[] finalInstances = instances;
            assertTrueEventually(() -> {
                for (HazelcastInstance instance : finalInstances) {
                    RaftService raftService = getRaftService(instance);
                    assertNull(raftService.getRaftNode(group2));

                    CPPersistenceServiceImpl cpPersistenceService = getCpPersistenceService(instance);
                    File groupDir = cpPersistenceService.getGroupDir(group2);
                    assertFalse(group2 + " directory '" + groupDir + "' should not exist!", groupDir.exists());
                }

                RaftService raftService = getRaftService(finalInstances[0]);
                Collection<CPGroupId> groupIds = raftService.getCPGroupIds().get();
                assertThat("Groups: " + groupIds, groupIds, hasSize(2));
                assertThat(groupIds, hasItem(group1));
                assertThat(groupIds, not(hasItem(group2)));

                assertEquals(group1, raftService.getCPGroup(group1.getName()).get().id());
                assertNull(raftService.getCPGroup(group2.getName()).get());
            });
        }
    }

    private static CPPersistenceServiceImpl getCpPersistenceService(HazelcastInstance instance) {
        return (CPPersistenceServiceImpl) getNode(instance).getNodeExtension().getCPPersistenceService();
    }

    private void destroyRaftGroup(HazelcastInstance instance, CPGroupId groupId) {
        RaftService service = getRaftService(instance);
        RaftInvocationManager invocationManager = service.getInvocationManager();
        invocationManager.invoke(service.getMetadataGroupId(), new TriggerDestroyRaftGroupOp(groupId)).join();
    }
}
