package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.Member;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.nio.Address;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberListReaderWriterTest extends MetadataReaderWriterTestBase {

    private Node node = mock(Node.class);

    @Override
    void setupInternal() {
        Address address = new Address("127.0.0.1", localAddress, 5701);
        MemberImpl localMember = new MemberImpl.Builder(address)
                .version(MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion()))
                .localMember(true)
                .uuid(newUnsecureUUID())
                .build();
        when(node.getLocalMember()).thenReturn(localMember);
    }

    @Test
    public void test_readNotExistingFolder() throws Exception {
        MemberListReader reader = new MemberListReader(getNonExistingFolder());
        reader.read();

        assertNull(reader.getLocalMember());
        assertTrue(reader.getMembers().isEmpty());
    }

    @Test
    public void test_readEmptyFolder() throws Exception {
        MemberListReader reader = new MemberListReader(folder);
        reader.read();

        assertNull(reader.getLocalMember());
        assertTrue(reader.getMembers().isEmpty());
    }

    @Test(expected = FileNotFoundException.class)
    public void test_writeNotExistingFolder() throws Exception {
        MemberListWriter writer = new MemberListWriter(getNonExistingFolder(), node);
        writer.write(Collections.emptyList());
    }

    @Test
    public void test_EmptyWriteRead() throws Exception {
        MemberListWriter writer = new MemberListWriter(folder, node);
        writer.write(Collections.emptyList());

        MemberListReader reader = new MemberListReader(folder);
        reader.read();

        assertEquals(node.getLocalMember(), reader.getLocalMember());
        assertTrue(reader.getMembers().isEmpty());
    }

    @Test
    public void test_WriteRead() throws Exception {
        Collection<Member> members = initializeMembers(100);
        MemberListWriter writer = new MemberListWriter(folder, node);
        writer.write(members);

        MemberListReader reader = new MemberListReader(folder);
        reader.read();

        assertEquals(node.getLocalMember(), reader.getLocalMember());

        Collection<MemberImpl> readMembers = reader.getMembers();
        assertNotNull(readMembers);
        assertEquals(members.size(), readMembers.size());
        assertEquals(members, readMembers);
    }

    @SuppressWarnings("SameParameterValue")
    private Collection<Member> initializeMembers(int memberCount) {
        PartitionReplica[] replicas = initializeReplicas(memberCount - 1);
        Collection<Member> members = new HashSet<>(memberCount);
        for (PartitionReplica replica : replicas) {
            MemberVersion version = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());
            members.add(new MemberImpl.Builder(replica.address())
                                .version(version)
                                .uuid(newUnsecureUUID())
                                .build());
        }
        members.add(new MemberImpl(node.getLocalMember()));
        return members;
    }
}
