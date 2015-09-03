package com.hazelcast.elasticmemory;

import com.hazelcast.internal.storage.Storage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.EmptyStatement;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;

import static java.lang.String.format;

public abstract class StorageFactorySupport implements StorageFactory {

    private static final String MAX_HEAP_MEMORY_PARAM = "-Xmx";
    private static final String MAX_DIRECT_MEMORY_PARAM = "-XX:MaxDirectMemorySize";

    static Storage<DataRefImpl> createStorage(String total, String chunk, boolean useUnsafe, ILogger logger) {
        MemorySize totalSize = MemorySize.parse(total);
        MemorySize chunkSize = MemorySize.parse(chunk);
        logger.info("Elastic-Memory off-heap storage total size: " + totalSize.megaBytes() + " megabytes");
        logger.info("Elastic-Memory off-heap storage chunk size: " + chunkSize.bytes() + " bytes");

        MemorySize jvmSize = getJvmDirectMemorySize(logger);
        if (jvmSize == null) {
            jvmSize = getJvmHeapMemorySize(logger);
        }
        if (jvmSize == null) {
            logger.warning(format("Either JVM max memory argument (%s) or max direct memory size argument (%s)"
                    + " should be configured in order to use Hazelcast Elastic Memory (e.g. java %s=1G -Xmx1G -cp ...)!"
                    + " Using defaults...", MAX_HEAP_MEMORY_PARAM, MAX_DIRECT_MEMORY_PARAM, MAX_DIRECT_MEMORY_PARAM));
            jvmSize = totalSize;
        }
        checkOffHeapParams(jvmSize, totalSize, chunkSize);

        if (useUnsafe) {
            if (unsafeAvailable()) {
                return new UnsafeStorage(totalSize.bytes(), (int) chunkSize.bytes());
            }
            logger.warning("Could not load sun.misc.Unsafe! Falling back to ByteBuffer storage implementation...");
        }
        return new ByteBufferStorage(totalSize.bytes(), (int) chunkSize.bytes());
    }

    static boolean unsafeAvailable() {
        try {
            return (UnsafeHelper.UNSAFE != null);
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
        }
        return false;
    }

    static void checkOffHeapParams(MemorySize jvm, MemorySize total, MemorySize chunk) {
        if (jvm.megaBytes() == 0) {
            throw new IllegalArgumentException(format("%s must be multitude of megabytes! Current: %d bytes",
                    MAX_DIRECT_MEMORY_PARAM, jvm.bytes()));
        }
        if (total.megaBytes() == 0) {
            throw new IllegalArgumentException(format("Elastic Memory total size must be a multitude of megabytes!"
                    + " Current: %d bytes", total.bytes()));
        }
        if (total.megaBytes() > jvm.megaBytes()) {
            throw new IllegalArgumentException(format("%s or %s must set to a greater value than Elastic Memory total size:"
                            + " %d megabytes, Total: %d megabytes", MAX_DIRECT_MEMORY_PARAM, MAX_HEAP_MEMORY_PARAM,
                    jvm.megaBytes(), total.megaBytes()));
        }
        if (total.bytes() <= chunk.bytes()) {
            throw new IllegalArgumentException(format("Elastic Memory total size must be greater than chunk size!"
                    + " Total: %d bytes, Chunk: %d bytes", total.bytes(), chunk.bytes()));
        }
    }

    static MemorySize getJvmDirectMemorySize(ILogger logger) {
        RuntimeMXBean rmx = ManagementFactory.getRuntimeMXBean();
        List<String> args = rmx.getInputArguments();
        for (String arg : args) {
            if (arg.startsWith(MAX_DIRECT_MEMORY_PARAM)) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Read JVM " + MAX_DIRECT_MEMORY_PARAM + " as: " + arg);
                }
                String[] tmp = arg.split("\\=");
                if (tmp.length == 2) {
                    String value = tmp[1];
                    return MemorySize.parse(value);
                }
                break;
            }
        }
        return null;
    }

    static MemorySize getJvmHeapMemorySize(ILogger logger) {
        RuntimeMXBean rmx = ManagementFactory.getRuntimeMXBean();
        List<String> args = rmx.getInputArguments();
        for (String arg : args) {
            if (arg.startsWith(MAX_HEAP_MEMORY_PARAM)) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Read JVM " + MAX_HEAP_MEMORY_PARAM + " as: " + arg);
                }
                String value = arg.substring(MAX_HEAP_MEMORY_PARAM.length());
                return MemorySize.parse(value);
            }
        }
        return null;
    }
}
