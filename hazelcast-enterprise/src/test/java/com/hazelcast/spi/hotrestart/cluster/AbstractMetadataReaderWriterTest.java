package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractMetadataReaderWriterTest extends MetadataReaderWriterTestBase {

    private static final String NAME = "sample.data";

    private static final int BUFFER_SIZE = 100;

    @Parameters(name = "dataSize: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {BUFFER_SIZE / 2},
                {BUFFER_SIZE - INT_SIZE_IN_BYTES},
                {BUFFER_SIZE},
                {BUFFER_SIZE * 2}
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
            for (byte b : param) {
                out.writeByte(b);
            }
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
        protected void doRead(DataInputStream in) throws IOException {
            int len = in.readInt();
            data = new byte[len];
            int read = in.read(data);
            assertEquals(len, read);
        }

        @Override
        protected String getFilename() {
            return NAME;
        }
    }
}
