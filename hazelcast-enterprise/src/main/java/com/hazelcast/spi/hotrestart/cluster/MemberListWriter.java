package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * Writes cluster member list to a specific file
 * by overwriting previous one if exists.
 */
class MemberListWriter extends AbstractMetadataWriter<Collection<Member>> {
    static final String FILE_NAME = "members.data";
    private static final String FILE_NAME_TMP = FILE_NAME + ".tmp";

    private final Address thisAddress;

    MemberListWriter(File homeDir, Address thisAddress) {
        super(homeDir);
        this.thisAddress = thisAddress;
    }

    @Override
    void doWrite(Collection<Member> members) throws IOException {
        writeAddress(thisAddress);
        writeInt(members.size());

        for (Member member : members) {
            writeAddress(member.getAddress());
        }
    }

    @Override
    String getFileName() {
        return FILE_NAME;
    }

    @Override
    String getNewFileName() {
        return FILE_NAME_TMP;
    }

}
