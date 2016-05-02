package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.nio.Address;

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
    private Address thisAddress;
    private Collection<Address> addresses = Collections.emptySet();

    MemberListReader(File homeDir) {
        super(homeDir);
    }

    @Override
    void doRead(DataInputStream in) throws IOException {
        thisAddress = readAddress(in);
        final int size = in.readInt();
        addresses = new ArrayList<Address>(size);
        for (int i = 0; i < size; i++) {
            addresses.add(readAddress(in));
        }
    }

    @Override
    String getFilename() {
        return MemberListWriter.FILE_NAME;
    }

    Address getThisAddress() {
        return thisAddress;
    }

    Collection<Address> getAddresses() {
        return addresses;
    }
}
