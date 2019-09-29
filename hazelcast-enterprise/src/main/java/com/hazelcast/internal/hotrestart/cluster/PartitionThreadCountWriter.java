package com.hazelcast.internal.hotrestart.cluster;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * Writes the partition thread count to disk.
 */
class PartitionThreadCountWriter extends AbstractMetadataWriter<Integer> {

    static final String FILE_NAME = "partition-thread-count.bin";

    PartitionThreadCountWriter(File homeDir) {
        super(homeDir);
    }

    static void writePartitionThreadCount(File homeDir, int threadCount) throws IOException {
        new PartitionThreadCountWriter(homeDir).write(threadCount);
    }

    @Override
    final String getFilename() {
        return FILE_NAME;
    }

    @Override
    final void doWrite(DataOutput out, Integer threadCount) throws IOException {
        out.writeInt(threadCount);
    }
}
