package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractMetadataReaderWriterTest extends MetadataReaderWriterTestBase {

    private static final String NAME = "sample.data";

    private static final int BUFFER_SIZE = 100;

    @Parameters(name = "dataSize: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BUFFER_SIZE / 2},
                {BUFFER_SIZE - INT_SIZE_IN_BYTES},
                {BUFFER_SIZE},
                {BUFFER_SIZE * 2},
        });
    }

    @Parameter
    public int dataSize;

    private byte[] data;

    @Override
    void setupInternal() {
        Random random = new Random();
        data = new byte[dataSize];
        random.nextBytes(data);
    }

    @Test
    public void test() throws Exception {
        SampleWriter writer = new SampleWriter(folder);
        writer.write(data);

        SampleReader reader = new SampleReader(folder);
        reader.read();

        assertArrayEquals(data, reader.data);
    }

    private static class SampleWriter extends AbstractMetadataWriter<byte[]> {

        SampleWriter(File homeDir) {
            super(homeDir);
        }

        @Override
        void doWrite(DataOutput out, byte[] param) throws IOException {
            out.writeInt(param.length);
            out.write(param);
        }

        @Override
        String getFilename() {
            return NAME;
        }
    }

    private static class SampleReader extends AbstractMetadataReader {

        private byte[] data;

        SampleReader(File homeDir) {
            super(homeDir);
        }

        @Override
        protected void doRead(DataInput in) throws IOException {
            int len = in.readInt();
            data = new byte[len];
            in.readFully(data);
        }

        @Override
        protected String getFilename() {
            return NAME;
        }
    }
}
