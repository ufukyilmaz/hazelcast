package com.hazelcast.internal.hotrestart.cluster;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;

/**
 * Reads the saved partition thread count.
 */
class PartitionThreadCountReader extends AbstractMetadataReader {

    private int loadedValue;

    PartitionThreadCountReader(File homeDir) {
        super(homeDir);
    }

    static int readPartitionThreadCount(File homeDir) throws IOException {
        final PartitionThreadCountReader r = new PartitionThreadCountReader(homeDir);
        r.read();
        return r.loadedValue;
    }

    @Override
    final String getFilename() {
        return PartitionThreadCountWriter.FILE_NAME;
    }

    @Override
    void doRead(DataInput in) throws IOException {
        this.loadedValue = in.readInt();
    }
}
