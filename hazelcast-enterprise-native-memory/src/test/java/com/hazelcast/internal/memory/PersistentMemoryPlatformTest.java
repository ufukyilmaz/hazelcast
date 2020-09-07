package com.hazelcast.internal.memory;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.LibMallocFactory;
import com.hazelcast.internal.memory.impl.MemkindMallocFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.internal.memory.PmemTestUtil.firstOf;
import static com.hazelcast.internal.util.OsHelper.isLinux;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PersistentMemoryPlatformTest extends ParameterizedMemoryTest {

    private final MemorySize nativeMemorySize = new MemorySize(32, MemoryUnit.MEGABYTES);

    @Test
    public void testPersistentMemoryFailOnNonLinuxPlatform() {
        NativeMemoryConfig config = new NativeMemoryConfig();
        PersistentMemoryDirectoryConfig directoryConfig = new PersistentMemoryDirectoryConfig(
                firstOf(PERSISTENT_MEMORY_DIRECTORIES));
        config.getPersistentMemoryConfig().addDirectoryConfig(directoryConfig);
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
