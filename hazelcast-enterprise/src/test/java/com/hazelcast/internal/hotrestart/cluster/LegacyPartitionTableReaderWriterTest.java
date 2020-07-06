package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.partition.ReadonlyInternalPartition;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;
import java.io.IOException;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
//RU_COMPAT_4_0
public class LegacyPartitionTableReaderWriterTest extends MetadataReaderWriterTestBase {

    @Test
    public void test_readNotExistingFolder() throws Exception {
        LegacyPartitionTableReader reader = new LegacyPartitionTableReader(getNonExistingFolder(), 100);
        reader.read();
        assertPartitionTableEmpty(reader.getPartitionTable());
    }

    @Test
    public void test_readEmptyFolder() throws Exception {
        LegacyPartitionTableReader reader = new LegacyPartitionTableReader(folder, 100);
        reader.read();
        PartitionTableView table = reader.getPartitionTable();
        assertEquals(0, table.version());
        assertPartitionTableEmpty(table);
    }

    private void assertPartitionTableEmpty(PartitionTableView table) {
        for (int i = 0; i < table.length(); i++) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                assertNull(table.getReplica(i, j));
            }
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void test_writeNotExistingFolder() throws Exception {
        PartitionTableView partitions = new PartitionTableView(new InternalPartition[100], 0);
        LegacyPartitionTableWriter writer = new LegacyPartitionTableWriter(getNonExistingFolder());
        writer.write(partitions);
    }

    @Test
    public void test_EmptyWriteRead() throws Exception {
        test_WriteRead(0);
    }

    @Test
    public void test_WriteRead_fewMembers() throws Exception {
        test_WriteRead(3);
    }

    @Test
    public void test_WriteRead_manyMembers() throws Exception {
        test_WriteRead(100);
    }

    private void test_WriteRead(int memberCount) throws Exception {
        PartitionReplica[] replicas = initializeReplicas(memberCount);

        final int partitionCount = 100;
        PartitionTableView expectedPartitionTable = initializePartitionTable(replicas, partitionCount);

        LegacyPartitionTableWriter writer = new LegacyPartitionTableWriter(folder);
        writer.write(expectedPartitionTable);

        LegacyPartitionTableReader reader = new LegacyPartitionTableReader(folder, partitionCount);
        reader.read();

        PartitionTableView partitionTable = reader.getPartitionTable();
        assertEquals(expectedPartitionTable, partitionTable);
    }

    @Test
    public void test_withIncreasingPartitionCount() throws Exception {
        PartitionReplica[] replicas = initializeReplicas(0);
        int partitionCount = 100;

        PartitionTableView expectedPartitionTable = initializePartitionTable(replicas, partitionCount);

        LegacyPartitionTableWriter writer = new LegacyPartitionTableWriter(folder);
        writer.write(expectedPartitionTable);

        int newPartitionCount = partitionCount + 1;
        LegacyPartitionTableReader reader = new LegacyPartitionTableReader(folder, newPartitionCount);

        try {
            reader.read();
            fail("Should fail to read partition table!");
        } catch (IOException expected) {
            ignore(expected);
        }
    }

    @Test
    public void test_withDecreasingPartitionCount() throws Exception {
        PartitionReplica[] replicas = initializeReplicas(0);
        int partitionCount = 100;

        PartitionTableView expectedPartitionTable = initializePartitionTable(replicas, partitionCount);

        LegacyPartitionTableWriter writer = new LegacyPartitionTableWriter(folder);
        writer.write(expectedPartitionTable);

        int newPartitionCount = partitionCount - 1;
        LegacyPartitionTableReader reader = new LegacyPartitionTableReader(folder, newPartitionCount);

        try {
            reader.read();
            fail("Should fail to read partition table!");
        } catch (IOException expected) {
            ignore(expected);
        }
    }

    public PartitionTableView initializePartitionTable(PartitionReplica[] members, int partitionCount) {
        InternalPartition[] partitions = new InternalPartition[partitionCount];

        int replicaCount = Math.min(members.length, MAX_REPLICA_COUNT);

        int[] versions = new int[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            PartitionReplica[] replicas = new PartitionReplica[MAX_REPLICA_COUNT];
            for (int j = 0; j < replicaCount; j++) {
                replicas[j] = members[(i + j) % members.length];
            }
            partitions[i] = new ReadonlyInternalPartition(replicas, i, 0);
        }
        return new PartitionTableView(partitions, RandomPicker.getInt(1, 100));
    }
}
