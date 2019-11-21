package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.internal.persistence.BufferedRaf.BufRafObjectDataIn;
import com.hazelcast.cp.internal.persistence.BufferedRaf.BufRafObjectDataOut;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.rename;

/**
 * Simple file IO support class for CP persistence.
 */
final class FileIOSupport {

    static final String TMP_SUFFIX = ".tmp";

    interface Writable {
        void writeTo(ObjectDataOutput out) throws IOException;
    }

    interface Readable<T> {
        T readFrom(ObjectDataInput in) throws IOException;
    }

    private FileIOSupport() {
    }

    /**
     * Writes the {@code writable} with CRC checksum to the specified file
     * in directory {@code baseDir} atomically.
     *
     * @param baseDir              base directory
     * @param filename             target file in {@code baseDir}
     * @param serializationService serialization service
     * @param writable             writable object
     */
    static void writeWithChecksum(File baseDir, String filename, InternalSerializationService serializationService,
            Writable writable) throws IOException {

        File tmpFile = new File(baseDir, filename + TMP_SUFFIX);
        BufferedRaf bufRaf = new BufferedRaf(new RandomAccessFile(tmpFile, "rw"));
        BufRafObjectDataOut out = bufRaf.asObjectDataOutputStream(serializationService);
        try {
            writable.writeTo(out);
            out.writeCrc32();
            out.flush();
            bufRaf.force();
        } finally {
            closeResource(out);
        }
        // We can use java.nio.file.Files#move with ATOMIC_MOVE.
        // Files.move(tmpFile.toPath(), baseDir.toPath().resolve(filename), REPLACE_EXISTING, ATOMIC_MOVE);
        rename(tmpFile, new File(baseDir, filename));
    }

    /**
     * Reads {@code readable} from the specified file in directory {@code baseDir}
     * and verifies the checksum.
     * <p>
     * If checksum verification fails,
     * a {@link com.hazelcast.cp.internal.raft.exception.LogValidationException LogValidationException} is thrown.
     *
     * @param baseDir              base directory
     * @param filename             source file in {@code baseDir}
     * @param serializationService serialization service
     * @param readable             readable object
     * @return result of read
     */
    static <T> T readWithChecksum(File baseDir, String filename, InternalSerializationService serializationService,
            Readable<T> readable) throws IOException {
        File f = new File(baseDir, filename);
        if (!f.exists()) {
            return null;
        }
        BufferedRaf raf = new BufferedRaf(new RandomAccessFile(f, "r"));
        BufRafObjectDataIn in = raf.asObjectDataInputStream(serializationService);
        try {
            T result = readable.readFrom(in);
            in.checkCrc32();
            return result;
        } finally {
            closeResource(in);
        }
    }
}
