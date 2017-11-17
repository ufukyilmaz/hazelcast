package com.hazelcast.spi.hotrestart.cluster;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

/**
 * Reads local member UUID from a specific file if exists.
 */
class LocalMemberReader extends MemberListReader {

    LocalMemberReader(File homeDir) {
        super(homeDir);
    }

    @Override
    void doRead(DataInputStream in) throws IOException {
        localMember = readMember(in);
    }
}
