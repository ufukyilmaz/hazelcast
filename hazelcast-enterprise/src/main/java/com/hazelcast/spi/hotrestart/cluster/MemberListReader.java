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
    private static final String FILE_NAME = MemberListWriter.FILE_NAME;

    private Address thisAddress;
    private Collection<Address> addresses = Collections.emptySet();

    MemberListReader(File homeDir) {
        super(homeDir);
    }

    @Override
    protected void doRead(DataInputStream in) throws IOException {
        thisAddress = readAddressFromStream(in);

        int size = in.readInt();
        addresses = new ArrayList<Address>(size);
        for (int i = 0; i < size; i++) {
            Address address = readAddressFromStream(in);
            addresses.add(address);
        }
    }

    @Override
    protected String getFileName() {
        return FILE_NAME;
    }

    Address getThisAddress() {
        return thisAddress;
    }

    Collection<Address> getAddresses() {
        return addresses;
    }
}
