package com.hazelcast.internal.hotrestart.cluster;

import java.io.DataInput;
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
    void doRead(DataInput in) throws IOException {
        localMember = readMember(in);
    }
}
