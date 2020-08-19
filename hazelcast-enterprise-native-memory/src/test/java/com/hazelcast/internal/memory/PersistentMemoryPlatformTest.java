package com.hazelcast.internal.memory;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.LibMallocFactory;
import com.hazelcast.internal.memory.impl.MemkindMallocFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import org.junit.Test;

import static com.hazelcast.internal.memory.PmemTestUtil.firstOf;
import static com.hazelcast.internal.util.OsHelper.isLinux;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PersistentMemoryPlatformTest extends ParameterizedMemoryTest {

    private final MemorySize nativeMemorySize = new MemorySize(32, MemoryUnit.MEGABYTES);

    @Test
    public void testPersistentMemoryFailOnNonLinuxPlatform() {
        NativeMemoryConfig config = new NativeMemoryConfig();
        config.setPersistentMemoryDirectory(firstOf(PERSISTENT_MEMORY_DIRECTORIES));
        LibMallocFactory libMallocFactory = new MemkindMallocFactory(config);

        LibMalloc libMalloc = null;

        try {
            libMalloc = libMallocFactory.create(nativeMemorySize.bytes());
            assertNotNull(libMalloc);
            assertTrue(isLinux());
        } catch (UnsupportedOperationException e) {
            assertTrue(!isLinux());
        } finally {
            if (libMalloc != null) {
                libMalloc.dispose();
            }
        }
    }
}
