/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberListReaderWriterTest extends AbstractReaderWriterTest {

    private Address thisAddress;

    @Override
    void setupInternal() {
        thisAddress = new Address("127.0.0.1", localAddress, 5000);
    }

    @Override
    void tearDownInternal() {
    }

    @Test
    public void test_readNotExistingFolder() throws IOException {
        MemberListReader reader = new MemberListReader(getNotExistingFolder());
        reader.read();

        assertNull(reader.getThisAddress());
        assertTrue(reader.getAddresses().isEmpty());
    }

    @Test
    public void test_readEmptyFolder() throws IOException {
        MemberListReader reader = new MemberListReader(folder);
        reader.read();

        assertNull(reader.getThisAddress());
        assertTrue(reader.getAddresses().isEmpty());
    }

    @Test(expected = FileNotFoundException.class)
    public void test_writeNotExistingFolder() throws IOException {
        MemberListWriter writer = new MemberListWriter(getNotExistingFolder(), thisAddress);
        writer.write(Collections.<Member>emptyList());
    }

    @Test
    public void test_EmptyWriteRead() throws IOException {
        MemberListWriter writer = new MemberListWriter(folder, thisAddress);
        writer.write(Collections.<Member>emptyList());

        MemberListReader reader = new MemberListReader(folder);
        reader.read();

        assertEquals(thisAddress, reader.getThisAddress());
        assertTrue(reader.getAddresses().isEmpty());
    }

    @Test
    public void test_WriteRead() throws IOException {
        Collection<Member> members = initializeMembers(100);
        MemberListWriter writer = new MemberListWriter(folder, thisAddress);
        writer.write(members);

        MemberListReader reader = new MemberListReader(folder);
        reader.read();

        assertEquals(thisAddress, reader.getThisAddress());
        Collection<Address> addresses = reader.getAddresses();

        assertNotNull(addresses);
        assertEquals(members.size(), addresses.size());

        Set<Address> expectedAddresses = toAddresses(members);
        assertEquals(expectedAddresses, new HashSet<Address>(addresses));
    }

    private Set<Address> toAddresses(Collection<Member> members) {
        Set<Address> expectedAddresses = new HashSet<Address>(members.size());
        for (Member member : members) {
            expectedAddresses.add(member.getAddress());
        }
        return expectedAddresses;
    }

    private Collection<Member> initializeMembers(int memberCount) {
        Address[] addresses = initializeAddresses(memberCount - 1);
        Collection<Member> members = new ArrayList<Member>(memberCount);
        for (Address address : addresses) {
            members.add(new MemberImpl(address, false));
        }
        members.add(new MemberImpl(thisAddress, true));
        return members;
    }
}
