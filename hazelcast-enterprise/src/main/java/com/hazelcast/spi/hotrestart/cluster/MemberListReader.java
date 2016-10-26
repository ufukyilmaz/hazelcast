package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Reads cluster member list from a specific file if exists.
 */
class MemberListReader extends AbstractMetadataReader {

    MemberImpl thisMember;
    private Collection<MemberImpl> members = Collections.emptySet();

    MemberListReader(File homeDir) {
        super(homeDir);
    }

    @Override
    void doRead(DataInputStream in) throws IOException {
        thisMember = readMember(in);
        int size = in.readInt();
        members = new ArrayList<MemberImpl>(size);
        for (int i = 0; i < size; i++) {
            MemberImpl member = readMember(in);
            members.add(member);
        }
    }

    static MemberImpl readMember(DataInput in) throws IOException {
        String uuid = in.readUTF();
        Address address = readAddress(in);
        boolean localMember = in.readBoolean();
        return new MemberImpl(address, localMember, uuid, null);
    }

    @Override
    String getFilename() {
        return MemberListWriter.FILE_NAME;
    }

    MemberImpl getThisMember() {
        return thisMember;
    }

    Collection<MemberImpl> getMembers() {
        return members;
    }
}
