package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.cluster.MemberClusterStartInfo.DataLoadStatus;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartStatusDTOUtil.create;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterHotRestartStatusDTOUtilTest extends HazelcastTestSupport {

    private MemberImpl localMember;
    private MemberImpl loadInProgressMember;
    private MemberImpl loadFailedMember;
    private MemberImpl loadSuccessfulMember;

    private Collection<MemberImpl> restoredMembers = new ArrayList<MemberImpl>();

    private ClusterMetadataManager manager;

    @Before
    public void setUp() throws Exception {
        localMember = new MemberImpl(new Address("127.0.0.1", 5701), MemberVersion.UNKNOWN, true);
        loadInProgressMember = new MemberImpl(new Address("127.0.0.1", 5702), MemberVersion.UNKNOWN, false);
        loadFailedMember = new MemberImpl(new Address("127.0.0.1", 5703), MemberVersion.UNKNOWN, false);
        loadSuccessfulMember = new MemberImpl(new Address("127.0.0.1", 5704), MemberVersion.UNKNOWN, false);

        restoredMembers.add(localMember);
        restoredMembers.add(loadInProgressMember);
        restoredMembers.add(loadFailedMember);
        restoredMembers.add(loadSuccessfulMember);

        manager = mock(ClusterMetadataManager.class);
        when(manager.getHotRestartStatus()).thenReturn(HotRestartClusterStartStatus.CLUSTER_START_IN_PROGRESS);
        when(manager.getClusterDataRecoveryPolicy()).thenReturn(HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
        when(manager.getRemainingValidationTimeMillis()).thenReturn(23L);
        when(manager.getRemainingDataLoadTimeMillis()).thenReturn(42L);

        when(manager.getRestoredMembers()).thenReturn(restoredMembers);
        when(manager.getMemberDataLoadStatus(loadInProgressMember)).thenReturn(DataLoadStatus.LOAD_IN_PROGRESS);
        when(manager.getMemberDataLoadStatus(loadFailedMember)).thenReturn(DataLoadStatus.LOAD_FAILED);
        when(manager.getMemberDataLoadStatus(loadSuccessfulMember)).thenReturn(DataLoadStatus.LOAD_SUCCESSFUL);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClusterHotRestartStatusDTOUtil.class);
    }

    @Test
    public void testCreate() {
        ClusterHotRestartStatusDTO dto = create(manager);

        assertEquals(HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY, dto.getDataRecoveryPolicy());
        assertEquals(ClusterHotRestartStatus.IN_PROGRESS, dto.getHotRestartStatus());
        assertEquals(23L, dto.getRemainingValidationTimeMillis());
        assertEquals(42L, dto.getRemainingDataLoadTimeMillis());

        Map<String, MemberHotRestartStatus> restartStatusMap = dto.getMemberHotRestartStatusMap();
        assertEquals(MemberHotRestartStatus.PENDING, restartStatusMap.get(getAddress(localMember)));
        assertEquals(MemberHotRestartStatus.LOAD_IN_PROGRESS, restartStatusMap.get(getAddress(loadInProgressMember)));
        assertEquals(MemberHotRestartStatus.FAILED, restartStatusMap.get(getAddress(loadFailedMember)));
        assertEquals(MemberHotRestartStatus.SUCCESSFUL, restartStatusMap.get(getAddress(loadSuccessfulMember)));
    }

    @Test
    public void testCreate_withSuccessfulState() {
        when(manager.getHotRestartStatus()).thenReturn(HotRestartClusterStartStatus.CLUSTER_START_SUCCEEDED);

        ClusterHotRestartStatusDTO dto = create(manager);

        assertEquals(ClusterHotRestartStatus.SUCCEEDED, dto.getHotRestartStatus());
    }

    @Test
    public void testCreate_withFailedState() {
        when(manager.getHotRestartStatus()).thenReturn(HotRestartClusterStartStatus.CLUSTER_START_FAILED);

        ClusterHotRestartStatusDTO dto = create(manager);

        assertEquals(ClusterHotRestartStatus.FAILED, dto.getHotRestartStatus());
    }

    private String getAddress(MemberImpl member) {
        return member.getAddress().getHost() + ":" + member.getAddress().getPort();
    }
}
