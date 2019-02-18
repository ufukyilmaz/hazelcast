package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.version.MemberVersion;

import java.io.DataInput;
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
    void doRead(DataInput in) throws IOException {
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
        MemberVersion version = MemberVersion.of(in.readUTF());
        return new MemberImpl.Builder(address)
                .version(version)
                .localMember(localMember)
                .uuid(uuid)
                .build();
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
