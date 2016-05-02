package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;

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

    private final Address thisAddress;

    MemberListWriter(File homeDir, Address thisAddress) {
        super(homeDir);
        this.thisAddress = thisAddress;
    }

    @Override
    String getFilename() {
        return FILE_NAME;
    }

    @Override
    void doWrite(DataOutput out, Collection<Member> members) throws IOException {
        writeAddress(out, thisAddress);
        out.writeInt(members.size());
        for (Member member : members) {
            writeAddress(out, member.getAddress());
        }
    }
}
