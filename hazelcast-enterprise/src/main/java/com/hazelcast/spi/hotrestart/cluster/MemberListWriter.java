package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.UUIDSerializationUtil;

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

    private Node node;

    MemberListWriter(File homeDir, Node node) {
        super(homeDir);
        this.node = node;
    }

    @Override
    synchronized void doWrite(DataOutput out, Collection<Member> members) throws IOException {
        writeMember(out, node.getLocalMember());
        out.writeInt(members.size());
        for (Member member : members) {
            writeMember(out, member);
        }
    }

    private void writeMember(DataOutput out, Member member) throws IOException {
        UUIDSerializationUtil.writeUUID(out, member.getUuid());
        writeAddress(out, member.getAddress());
        out.writeBoolean(member.localMember());
        out.writeUTF(member.getVersion().toString());
    }

    @Override
    String getFilename() {
        return FILE_NAME;
    }
}
