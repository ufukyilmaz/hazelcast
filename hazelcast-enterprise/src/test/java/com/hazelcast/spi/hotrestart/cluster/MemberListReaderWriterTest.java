package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.core.Member;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static com.hazelcast.instance.BuildInfoProvider.BUILD_INFO;
import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberListReaderWriterTest extends MetadataReaderWriterTestBase {

    private Node node = mock(Node.class);

    @Override
    void setupInternal() {
        Address address = new Address("127.0.0.1", localAddress, 5701);
        MemberImpl localMember = new MemberImpl(address, Version.of(BUILD_INFO.getVersion()), true,
                newUnsecureUuidString(), null);
        when(node.getLocalMember()).thenReturn(localMember);
    }

    @Test
    public void test_readNotExistingFolder() throws IOException {
        MemberListReader reader = new MemberListReader(getNonExistingFolder());
        reader.read();

        assertNull(reader.getLocalMember());
        assertTrue(reader.getMembers().isEmpty());
    }

    @Test
    public void test_readEmptyFolder() throws IOException {
        MemberListReader reader = new MemberListReader(folder);
        reader.read();

        assertNull(reader.getLocalMember());
        assertTrue(reader.getMembers().isEmpty());
    }

    @Test(expected = FileNotFoundException.class)
    public void test_writeNotExistingFolder() throws IOException {
        MemberListWriter writer = new MemberListWriter(getNonExistingFolder(), node);
        writer.write(Collections.<Member>emptyList());
    }

    @Test
    public void test_EmptyWriteRead() throws IOException {
        MemberListWriter writer = new MemberListWriter(folder, node);
        writer.write(Collections.<Member>emptyList());

        MemberListReader reader = new MemberListReader(folder);
        reader.read();

        assertEquals(node.getLocalMember(), reader.getLocalMember());
        assertTrue(reader.getMembers().isEmpty());
    }

    @Test
    public void test_WriteRead() throws IOException {
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

    private Collection<Member> initializeMembers(int memberCount) {
        Address[] addresses = initializeAddresses(memberCount - 1);
        Collection<Member> members = new HashSet<Member>(memberCount);
        for (Address address : addresses) {
            members.add(new MemberImpl(address, Version.of(BUILD_INFO.getVersion()), false, newUnsecureUuidString(), null));
        }
        members.add(new MemberImpl(node.getLocalMember()));
        return members;
    }
}
