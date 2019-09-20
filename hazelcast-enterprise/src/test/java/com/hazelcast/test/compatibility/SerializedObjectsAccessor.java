package com.hazelcast.test.compatibility;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.test.compatibility.SamplingRunListener.INDEX_FILE_SUFFIX;
import static com.hazelcast.test.compatibility.SamplingRunListener.SAMPLES_FILE_SUFFIX;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.lang.Integer.parseInt;

/**
 * Random access to a file containing serialized object samples captured by {@link SamplingSerializationService}
 */
@SuppressWarnings("WeakerAccess")
public class SerializedObjectsAccessor implements Closeable, Iterable<SerializedObjectsAccessor.SerializedObject> {

    // filesystem path to index/samples file excluding suffix
    private final String samplesPath;

    // className -> samples positions
    private final Map<String, SamplePosition[]> index;

    private final RandomAccessFile samplesFile;

    public SerializedObjectsAccessor(String samplesResourcePath) {
        samplesPath = resourceToFilePath(samplesResourcePath);
        index = new HashMap<String, SamplePosition[]>(1000);
        readIndex();
        try {
            samplesFile = new RandomAccessFile(this.samplesPath + SAMPLES_FILE_SUFFIX, "r");
        } catch (FileNotFoundException e) {
            throw rethrow(e);
        }
    }

    public byte[] bytesFor(String className, int sampleIndex) {
        if (!index.containsKey(className)) {
            return null;
        }

        SamplePosition samplePosition = index.get(className)[sampleIndex];
        byte[] sample = new byte[samplePosition.length];
        try {
            samplesFile.seek(samplePosition.getOffset());
            int bytesRead = samplesFile.read(sample);
            if (bytesRead != samplePosition.getLength()) {
                throw new RuntimeException("Could not read " + samplePosition.getLength() + " bytes from offset "
                        + samplePosition.getOffset());
            }
            return sample;
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    @Override
    public Iterator<SerializedObject> iterator() {
        return new SerializedObjectIterator(index.keySet().iterator());
    }

    @Override
    public void close() throws IOException {
        samplesFile.close();
    }

    private String resourceToFilePath(String samplesResourcePath) {
        URL url = SerializedObjectsAccessor.class.getClassLoader().getResource(samplesResourcePath + INDEX_FILE_SUFFIX);
        return url.getFile().replace(".index", "");
    }

    private void readIndex() {
        try {
            FileReader reader = new FileReader(samplesPath + INDEX_FILE_SUFFIX);
            BufferedReader lineReader = new BufferedReader(reader);
            String nextLine;
            while ((nextLine = lineReader.readLine()) != null) {
                if (nextLine.startsWith("#") || nextLine.isEmpty()) {
                    // ignore the line
                    continue;
                }
                String[] tokens = nextLine.split(",");
                String className = tokens[0];
                List<SamplePosition> samplePositions = new ArrayList<SamplePosition>();
                int offset = 0;
                int length;
                for (int i = 1; i < tokens.length; i++) {
                    // even index is position, odd index is length
                    if (i % 2 == 0) {
                        length = parseInt(tokens[i]);
                        samplePositions.add(new SamplePosition(offset, length));
                    } else {
                        offset = parseInt(tokens[i]);
                    }
                }
                index.put(className, samplePositions.toArray(new SamplePosition[0]));
            }
            lineReader.close();
        } catch (FileNotFoundException e) {
            rethrow(e);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class SamplePosition {

        private final int offset;
        private final int length;

        SamplePosition(int offset, int length) {
            this.offset = offset;
            this.length = length;
        }

        int getOffset() {
            return offset;
        }

        int getLength() {
            return length;
        }
    }

    public class SerializedObjectIterator implements Iterator<SerializedObject> {

        private final Iterator<String> classNamesIterator;

        // state of iteration: current class name, current sample index
        private String className;
        private int sampleIndex;

        SerializedObjectIterator(Iterator<String> classNamesIterator) {
            this.classNamesIterator = classNamesIterator;
        }

        @Override
        public boolean hasNext() {
            if (className == null) {
                return classNamesIterator.hasNext();
            }

            SamplePosition[] availableSamples = index.get(className);
            if (sampleIndex < availableSamples.length) {
                return true;
            } else if (sampleIndex == availableSamples.length) {
                return classNamesIterator.hasNext();
            } else {
                throw new IllegalStateException(String.format("Invalid sample index %d", sampleIndex));
            }
        }

        @Override
        public SerializedObject next() {
            if (className == null) {
                className = classNamesIterator.next();
            }
            SamplePosition[] availableSamples = index.get(className);

            if (sampleIndex == availableSamples.length) {
                // done with current class name, advance to next class name and reset sample index
                className = classNamesIterator.next();
                sampleIndex = 0;
            }

            SerializedObject serializedObject = new SerializedObject(className, sampleIndex,
                    bytesFor(className, sampleIndex));

            sampleIndex++;
            return serializedObject;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

    static class SerializedObject {

        private final String className;
        private final int index;
        private final byte[] bytes;

        SerializedObject(String className, int index, byte[] bytes) {
            this.className = className;
            this.index = index;
            this.bytes = bytes;
        }

        String getClassName() {
            return className;
        }

        int getIndex() {
            return index;
        }

        byte[] getBytes() {
            return bytes;
        }
    }
}
