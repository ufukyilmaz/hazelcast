package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberListReaderWriterTest extends MetadataReaderWriterTestBase {

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstance instance;

    @Override
    void setupInternal() {
        factory = new TestHazelcastInstanceFactory(1);
        instance = factory.newHazelcastInstance();
    }

    @Override
    void tearDownInternal() {
        factory.terminateAll();
        factory = null;
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
        MemberListWriter writer = new MemberListWriter(getNonExistingFolder(), getNode(instance));
        writer.write(Collections.<Member>emptyList());
    }

    @Test
    public void test_EmptyWriteRead() throws IOException {
        MemberListWriter writer = new MemberListWriter(folder, getNode(instance));
        writer.write(Collections.<Member>emptyList());

        MemberListReader reader = new MemberListReader(folder);
        reader.read();

        assertEquals(getNode(instance).getLocalMember(), reader.getLocalMember());
        assertTrue(reader.getMembers().isEmpty());
    }

    @Test
    public void test_WriteRead() throws IOException {
        Collection<Member> members = initializeMembers(100);
        MemberListWriter writer = new MemberListWriter(folder, getNode(instance));
        writer.write(members);

        MemberListReader reader = new MemberListReader(folder);
        reader.read();

        assertEquals(getNode(instance).getLocalMember(), reader.getLocalMember());

        Collection<MemberImpl> readMembers = reader.getMembers();
        assertNotNull(readMembers);
        assertEquals(members.size(), readMembers.size());
        assertEquals(members, readMembers);
    }

    private Collection<Member> initializeMembers(int memberCount) {
        Address[] addresses = initializeAddresses(memberCount - 1);
        Collection<Member> members = new HashSet<Member>(memberCount);
        for (Address address : addresses) {
            members.add(new MemberImpl(address, false, UuidUtil.newUnsecureUuidString(), null));
        }
        members.add(new MemberImpl(getNode(instance).getLocalMember()));
        return members;
    }
}
