package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.version.Version;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

/**
 * Reads cluster member list from a specific file if exists.
 */
class MemberListReader extends AbstractMetadataReader {

    MemberImpl localMember;
    private Collection<MemberImpl> members = Collections.emptySet();

    MemberListReader(File homeDir) {
        super(homeDir);
    }

    @Override
    void doRead(DataInputStream in) throws IOException {
        localMember = readMember(in);
        int size = in.readInt();
        members = new HashSet<MemberImpl>(size);
        for (int i = 0; i < size; i++) {
            MemberImpl member = readMember(in);
            members.add(member);
        }
    }

    static MemberImpl readMember(DataInput in) throws IOException {
        String uuid = in.readUTF();
        Address address = readAddress(in);
        boolean localMember = in.readBoolean();
        Version version = Version.of(in.readUTF());
        return new MemberImpl(address, version, localMember, uuid, null);
    }

    @Override
    String getFilename() {
        return MemberListWriter.FILE_NAME;
    }

    MemberImpl getLocalMember() {
        return localMember;
    }

    Collection<MemberImpl> getMembers() {
        return members;
    }
}
