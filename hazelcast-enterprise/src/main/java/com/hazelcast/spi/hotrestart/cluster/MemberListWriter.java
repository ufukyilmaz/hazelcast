package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.core.Member;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * Writes cluster member list to a specific file
 * by overwriting previous one if exists.
 */
class MemberListWriter extends AbstractMetadataWriter<Collection<Member>> {
    static final String FILE_NAME = "members.bin";

    private Member localMember;

    MemberListWriter(File homeDir) {
        super(homeDir);
    }

    @Override
    synchronized void doWrite(DataOutput out, Collection<Member> members) throws IOException {
        assert localMember != null : "Local member is not set!";
        writeMember(out, localMember);
        out.writeInt(members.size());
        for (Member member : members) {
            writeMember(out, member);
        }
    }

    synchronized void setLocalMember(Member member) {
        assert localMember == null : "Local member is already set! Current: " + localMember + ", New: " + member;
        localMember = member;
    }

    private void writeMember(DataOutput out, Member member) throws IOException {
        out.writeUTF(member.getUuid());
        writeAddress(out, member.getAddress());
        out.writeBoolean(member.localMember());
    }

    @Override
    String getFilename() {
        return FILE_NAME;
    }
}
