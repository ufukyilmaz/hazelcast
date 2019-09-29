package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;

import static com.hazelcast.internal.hotrestart.cluster.PartitionThreadCountReader.readPartitionThreadCount;
import static com.hazelcast.internal.hotrestart.cluster.PartitionThreadCountWriter.writePartitionThreadCount;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ThreadCountReaderWriterTest extends MetadataReaderWriterTestBase {

    private final int mockThreadCount = 7;

    @Test
    public void when_folderNotExist_thenReadReturn0() throws Exception {
        assertEquals(0, readPartitionThreadCount(folder));
    }

    @Test
    public void when_folderEmpty_thenReadReturn0() throws Exception {
        assertEquals(0, readPartitionThreadCount(getNonExistingFolder()));
    }

    @Test(expected = FileNotFoundException.class)
    public void when_folderNotExist_thenWriteFails() throws Exception {
        writePartitionThreadCount(getNonExistingFolder(), mockThreadCount);
    }

    @Test
    public void when_writeCount_thenReadAsWritten() throws Exception {
        writePartitionThreadCount(folder, mockThreadCount);
        assertEquals(mockThreadCount, readPartitionThreadCount(folder));
    }
}
