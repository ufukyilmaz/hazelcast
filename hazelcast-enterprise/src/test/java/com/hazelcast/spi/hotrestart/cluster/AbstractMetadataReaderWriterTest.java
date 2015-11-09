/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractMetadataReaderWriterTest extends AbstractReaderWriterTest {

    private static final String NAME = "sample.data";

    private static final int BUFFER_SIZE = 100;

    @Parameterized.Parameters(name = "dataSize: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {BUFFER_SIZE / 2},
                {BUFFER_SIZE - INT_SIZE_IN_BYTES},
                {BUFFER_SIZE},
                {BUFFER_SIZE * 2}
        });
    }

    @Parameterized.Parameter
    public int dataSize;

    private byte[] data;

    @Override
    void setupInternal() {
        Random random = new Random();
        data = new byte[dataSize];
        random.nextBytes(data);
    }

    @Test
    public void test() throws IOException {
        SampleWriter writer = new SampleWriter(folder, BUFFER_SIZE);
        writer.write(data);

        SampleReader reader = new SampleReader(folder);
        reader.read();

        assertArrayEquals(data, reader.data);
    }

    private static class SampleWriter extends AbstractMetadataWriter<byte[]> {

        SampleWriter(File homeDir, int bufferSize) {
            super(homeDir, bufferSize);
        }

        @Override
        void doWrite(byte[] param) throws IOException {
            writeInt(param.length);
            for (byte b : param) {
                writeByte(b);
            }
        }

        @Override
        String getFileName() {
            return NAME;
        }

        @Override
        String getNewFileName() {
            return NAME + ".tmp";
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
        protected String getFileName() {
            return NAME;
        }
    }
}
